package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"

	"github.com/fsnotify/fsnotify"
	"github.com/juju/errors"
	"github.com/juju/names"
	"github.com/juju/utils/set"
	"github.com/julienschmidt/httprouter"
	"golang.org/x/net/context"
	"golang.org/x/sync/errgroup"
)

const defaultAddr = ":19090"

var (
	agentsDirFlag = flag.String("d", "/var/lib/juju/agents", "Path to Juju agents directory")
	addrFlag      = flag.String("addr", defaultAddr, "Address to listen for connections on")
	hook          = flag.String("hook", "", "Path to a command to execute when an agent is added or removed.")
)

func Main() error {
	agents := agentsHandler{
		rp: httputil.ReverseProxy{
			Director:  agentDirector,
			Transport: newAgentTransport(),
		},
		agents: make(set.Strings),
	}

	watcher, err := initAgents(&agents)
	if err != nil {
		return errors.Annotate(err, "initialised agents")
	}
	defer watcher.Close()

	g, ctx := errgroup.WithContext(context.Background())
	g.Go(func() error {
		// Watch for addition and removal of agents, and fire hooks.
		// If a hook fails, then the process will exit with an error.
		// This gives us a way of guaranteeing delivery of hooks.
		err := watchAgents(&agents, watcher, ctx)
		return errors.Annotate(err, "watching agents")
	})
	g.Go(func() error {
		err := serveHTTP(&agents, ctx)
		return errors.Annotate(err, "serving HTTP")
	})
	return g.Wait()
}

type agentsHandler struct {
	rp httputil.ReverseProxy

	mu     sync.Mutex
	agents set.Strings
}

func (h *agentsHandler) addAgent(tag string) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.agents.Contains(tag) {
		return nil
	}
	if err := h.runHook("add", tag); err != nil {
		return errors.Trace(err)
	}
	h.agents.Add(tag)
	log.Println("Agent added:", tag)
	return nil
}

func (h *agentsHandler) removeAgent(tag string) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if !h.agents.Contains(tag) {
		return nil
	}
	if err := h.runHook("remove", tag); err != nil {
		return errors.Trace(err)
	}
	h.agents.Remove(tag)
	log.Println("Agent removed:", tag)
	return nil
}

func (h *agentsHandler) runHook(op, agent string) error {
	if *hook == "" {
		return nil
	}
	cmd := exec.Command(*hook, op, agent)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	err := cmd.Run()
	return errors.Annotatef(err, "running hook: %q %q", *hook, op+" "+agent)
}

func (h *agentsHandler) hasAgent(tag string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.agents.Contains(tag)
}

func (h *agentsHandler) currentAgents() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.agents.SortedValues()
}

func (h *agentsHandler) GetAgent(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	tag := ps.ByName("tag")
	if !h.hasAgent(tag) {
		http.Error(w, fmt.Sprintf("agent %q not found", tag), http.StatusNotFound)
		return
	}
	h.rp.ServeHTTP(w, r)
}

func (h *agentsHandler) ListAgents(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(h.currentAgents()); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Add("Content-Type", "application/json")
	buf.WriteTo(w)
}

// agentDirector is a function suitable for use as
// the httputil.ReverseProxy.Director value. It will
// set Host to the agent tag, and Path to everything
// following the agent name.
func agentDirector(req *http.Request) {
	const prefix = "/agents/"
	path := req.URL.Path[len(prefix):]
	pos := strings.IndexRune(path, '/')
	var agentTag string
	if pos == -1 {
		agentTag = path
		path = "/"
	} else {
		agentTag = path[:pos]
		path = path[pos:]
	}
	req.URL.Scheme = "http"
	req.URL.Path = path
	req.URL.Host = agentTag
	if _, ok := req.Header["User-Agent"]; !ok {
		req.Header.Set("User-Agent", "")
	}
}

type agentTransport struct {
	http.Transport
}

func newAgentTransport() *agentTransport {
	return &agentTransport{
		http.Transport{
			Dial: func(proto, addr string) (net.Conn, error) {
				tag, _, err := net.SplitHostPort(addr)
				if err != nil {
					return nil, err
				}
				return net.Dial("unix", agentSocket(tag))
			},
		},
	}
}

func (t *agentTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	resp, err := t.Transport.RoundTrip(r)
	if err != nil {
		return nil, err
	}
	// Rewrite the Location header to be relative to the agent root.
	loc := resp.Header.Get("Location")
	if strings.HasPrefix(loc, "/") {
		prefix := fmt.Sprintf("/agents/%s", r.URL.Host)
		loc = prefix + loc
		resp.Header.Set("Location", loc)
	}
	return resp, nil
}

func agentSocket(tag string) string {
	return "@jujud-" + tag
}

func initAgents(agents *agentsHandler) (*fsnotify.Watcher, error) {
	// Start a watcher for the agents dir.
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, errors.Annotate(err, "creating fsnotify watcher")
	}
	if err := watcher.Add(*agentsDirFlag); err != nil {
		return nil, errors.Annotate(err, "watching agents dir")
	}

	// List the current entries in the agents dir. We must do this
	// after starting the watcher to avoid missing any events.
	dir, err := os.Open(*agentsDirFlag)
	if err != nil {
		return nil, errors.Annotate(err, "opening agents dir")
	}
	defer dir.Close()

	entries, err := dir.Readdir(-1)
	if err != nil {
		return nil, errors.Annotate(err, "reading agents dir")
	}
	for _, info := range entries {
		name := info.Name()
		if _, err := names.ParseTag(name); err != nil {
			log.Printf("ERROR: %s", err)
			continue
		}
		if err := agents.addAgent(name); err != nil {
			watcher.Close()
			return nil, err
		}
	}

	return watcher, nil
}

func watchAgents(
	agents *agentsHandler,
	watcher *fsnotify.Watcher,
	ctx context.Context,
) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case event := <-watcher.Events:
			name := filepath.Base(event.Name)
			if _, err := names.ParseTag(name); err != nil {
				log.Printf("ERROR: %s", err)
				continue
			}
			switch event.Op {
			case fsnotify.Create:
				if err := agents.addAgent(name); err != nil {
					return errors.Trace(err)
				}
			case fsnotify.Remove:
				if err := agents.removeAgent(name); err != nil {
					return errors.Trace(err)
				}
			}
		case err := <-watcher.Errors:
			log.Println("ERROR:", err)
		}
	}
}

func serveHTTP(agents *agentsHandler, ctx context.Context) error {
	router := httprouter.New()
	router.GET("/agents/:tag", agents.GetAgent)
	router.GET("/agents/:tag/*path", agents.GetAgent)
	router.GET("/agents", agents.ListAgents)

	server := http.Server{
		Addr:    *addrFlag,
		Handler: router,
	}
	listener, err := net.Listen("tcp", *addrFlag)
	if err != nil {
		return errors.Annotate(err, "creating listener")
	}
	defer listener.Close()

	go func() {
		<-ctx.Done()
		listener.Close()
	}()
	return errors.Trace(server.Serve(listener))
}

func main() {
	flag.Parse()
	if err := Main(); err != nil {
		log.Fatal(err)
	}
}
