package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"time"

	"go.mongodb.org/mongo-driver/mongo"

	"github.com/percona-lab/percona-mongolink/errors"
	"github.com/percona-lab/percona-mongolink/log"
	"github.com/percona-lab/percona-mongolink/repl"
	"github.com/percona-lab/percona-mongolink/topo"
)

func main() {
	slog.SetLogLoggerLevel(slog.LevelDebug)

	ctx := context.Background()

	srvOpts := serverOptions{}
	flag.StringVar(&srvOpts.Port, "port", "2242", "port")
	flag.StringVar(&srvOpts.SourceURI, "src", "", "MongoDB connection string")
	flag.StringVar(&srvOpts.DestURI, "dest", "", "MongoDB connection string")
	flag.Parse()

	addr, err := buildServerAddr(srvOpts.Port)
	if err != nil {

		log.Error(ctx, "build server address", log.Err(err))
		os.Exit(1)
	}

	srv, err := newServer(ctx, srvOpts)
	if err != nil {
		log.Error(ctx, "server", log.Err(err))
		os.Exit(1)
	}

	httpServer := http.Server{
		Addr:    addr,
		Handler: srv.Handler(),

		ReadTimeout:       30 * time.Second,
		ReadHeaderTimeout: 3 * time.Second,
	}

	log.Info(ctx, fmt.Sprintf("starting server at %s", addr))
	err = httpServer.ListenAndServe()
	if err != nil {
		log.Error(ctx, "server", log.Err(err))
		os.Exit(1)
	}

	if err1 := srv.Close(ctx); err1 != nil {
		log.Error(ctx, "close server", log.Err(err1))
	}
}

type serverOptions struct {
	Port      string
	SourceURI string
	DestURI   string
}

var errUnsupportedPortRange = errors.New("port value is outside supported range [1024 - 65535]")

func buildServerAddr(port string) (string, error) {
	i, err := strconv.ParseInt(port, 10, 32)
	if err != nil {
		return "", errors.Wrap(err, "invalid port value format")
	}

	if i < 1024 || i > 65535 {
		return "", errUnsupportedPortRange
	}

	return "localhost:" + port, nil
}

type server struct {
	srcCluster *mongo.Client
	dstCluster *mongo.Client

	repl *repl.Replicator
}

func newServer(ctx context.Context, options serverOptions) (*server, error) {
	src, err := topo.Connect(ctx, options.SourceURI)
	if err != nil {
		return nil, errors.Wrap(err, "connect to source cluster")
	}
	log.Debug(ctx, "connected to source cluster")

	dst, err := topo.Connect(ctx, options.DestURI)
	if err != nil {
		return nil, errors.Wrap(err, "connect to destination cluster")
	}
	log.Debug(ctx, "connected to destination cluster")

	s := &server{
		srcCluster: src,
		dstCluster: dst,
		repl:       repl.New(src, dst),
	}
	return s, nil
}

func (s *server) Close(ctx context.Context) error {
	err0 := s.srcCluster.Disconnect(ctx)
	err1 := s.dstCluster.Disconnect(ctx)
	return errors.Join(err0, err1)
}

func (s *server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/start", s.handleStart)
	mux.HandleFunc("/finalize", s.handleFinalize)
	mux.HandleFunc("/status", s.handleStatus)
	return mux
}

func (s *server) handleStart(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	log.Info(ctx, "/start")

	err := s.repl.Start(ctx)
	if err != nil {
		log.Error(ctx, "start replication", log.Err(err))
		err := json.NewEncoder(w).Encode(startReponse{Error: err.Error()})
		if err != nil {
			log.Error(ctx, "write status", log.Err(err))
			internalServerError(w)
		}
		return
	}

	err = json.NewEncoder(w).Encode(startReponse{Ok: true})
	if err != nil {
		log.Error(ctx, "write status", log.Err(err))
		internalServerError(w)
	}
}

func (s *server) handleFinalize(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	log.Info(ctx, "/finalize")

	err := s.repl.Finalize(ctx)
	if err != nil {
		log.Error(ctx, "finalize replication", log.Err(err))
		err := json.NewEncoder(w).Encode(finalizeReponse{Error: err.Error()})
		if err != nil {
			log.Error(ctx, "write status", log.Err(err))
			internalServerError(w)
		}
		return
	}

	err = json.NewEncoder(w).Encode(finalizeReponse{Ok: true})
	if err != nil {
		log.Error(ctx, "write status", log.Err(err))
		internalServerError(w)
	}
}

func (s *server) handleStatus(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	log.Info(ctx, "/status")

	replStatus, err := s.repl.Status(ctx)
	if err != nil {
		log.Error(ctx, "replication status", log.Err(err))
		err := json.NewEncoder(w).Encode(statusResponse{Error: err.Error()})
		if err != nil {
			log.Error(ctx, "write status", log.Err(err))
			internalServerError(w)
		}
		return
	}

	res := statusResponse{
		Ok:    true,
		State: replStatus.State,
	}
	if !replStatus.LastAppliedOpTime.IsZero() {
		res.LastAppliedOpTime = fmt.Sprintf("%d.%d",
			replStatus.LastAppliedOpTime.T,
			replStatus.LastAppliedOpTime.I)
	}
	err = json.NewEncoder(w).Encode(res)
	if err != nil {
		log.Error(ctx, "write status", log.Err(err))
		internalServerError(w)
	}
}

type startReponse struct {
	Ok    bool   `json:"ok"`
	Error string `json:"error,omitempty"`
}

type finalizeReponse struct {
	Ok    bool   `json:"ok"`
	Error string `json:"error,omitempty"`
}

type statusResponse struct {
	Ok    bool   `json:"ok"`
	Error string `json:"error,omitempty"`

	State             repl.State `json:"state"`
	LastAppliedOpTime string     `json:"lastAppliedOpTime,omitempty"`
}

func internalServerError(w http.ResponseWriter) {
	http.Error(w,
		http.StatusText(http.StatusInternalServerError),
		http.StatusInternalServerError)
}
