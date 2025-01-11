package repl

import (
	"context"
	"fmt"
	"sync"

	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/percona-lab/percona-mongolink/errors"
	"github.com/percona-lab/percona-mongolink/log"
	"github.com/percona-lab/percona-mongolink/topo"
)

type State string

const (
	IdleState       = "idle"
	RunningState    = "running"
	CompletingState = "completing"
	CompletedState  = "completed"
)

type Status struct {
	State             State
	LastAppliedOpTime primitive.Timestamp
}

type Replicator struct {
	src  *mongo.Client
	dest *mongo.Client

	stopC chan struct{}

	state State

	lastAppliedOpTime primitive.Timestamp

	mu sync.Mutex
}

func New(source, target *mongo.Client) *Replicator {
	r := &Replicator{
		src:   source,
		dest:  target,
		state: IdleState,
	}
	return r
}

func (r *Replicator) Start(ctx context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	switch r.state {
	case IdleState:
	case CompletedState:
	default:
		return errors.Errorf("wrong status %q. already running", r.state)
	}

	r.stopC = make(chan struct{})
	r.state = RunningState
	r.lastAppliedOpTime = primitive.Timestamp{}

	go func() {
		err := r.do(context.TODO()) //nolint:contextcheck
		if err != nil {
			log.Error(ctx, "run", log.Err(err))
		}

		log.Info(ctx, "completed")
		r.mu.Lock()
		r.state = CompletedState
		r.mu.Unlock()
	}()

	return nil
}

func (r *Replicator) do(ctx context.Context) error {
	log.Info(ctx, "starting data cloning")

	startedAt, err := topo.ClusterTime(ctx, r.src)
	if err != nil {
		return errors.Wrap(err, "get cluster time")
	}

	cloner := dataCloner{
		Source:      r.src,
		Destination: r.dest,
		Drop:        true, // todo: expose
	}

	err = cloner.Clone(ctx)
	if err != nil {
		return errors.Wrap(err, "close")
	}

	err = cloner.BuildIndexes(ctx)
	if err != nil {
		return errors.Wrap(err, "build indexes")
	}

	log.Info(ctx, fmt.Sprintf("starting change application at %d.%d", startedAt.T, startedAt.I))
	err = r.runChangeApplication(ctx, startedAt)
	if err != nil {
		return errors.Wrap(err, "run change application")
	}

	return nil
}

func (r *Replicator) Finalize(ctx context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.state != RunningState {
		return errors.Errorf("wrong status %q. expected %q", r.state, RunningState)
	}

	r.state = CompletingState
	close(r.stopC)
	return nil
}

func (r *Replicator) Status(ctx context.Context) (*Status, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	s := &Status{
		State:             r.state,
		LastAppliedOpTime: r.lastAppliedOpTime,
	}
	return s, nil
}

func (r *Replicator) runChangeApplication(
	ctx context.Context,
	startAt primitive.Timestamp,
) error {
	applier := &EventApplier{Client: r.dest}
	opts := options.ChangeStream().
		SetStartAtOperationTime(&startAt).
		SetShowExpandedEvents(true)
	cur, err := r.src.Watch(ctx, mongo.Pipeline{}, opts)
	if err != nil {
		return errors.Wrap(err, "start change stream")
	}
	defer cur.Close(ctx)

	var optime primitive.Timestamp
	for {
		select {
		case <-r.stopC:
			r.mu.Lock()
			r.state = CompletedState
			r.mu.Unlock()
			return nil
		default:
		}

		for cur.TryNext(ctx) {
			optime, err = applier.Apply(ctx, cur.Current)
			if err != nil || IsInvalidatedError(err) || IsUnsupportedEventError(err) {
				r.mu.Lock()
				r.lastAppliedOpTime = optime
				r.mu.Unlock()
			}

			if IsInvalidatedError(err) {
				opts := options.ChangeStream().
					SetResumeAfter(cur.ResumeToken()).
					SetShowExpandedEvents(true)
				cur, err = r.src.Watch(ctx, mongo.Pipeline{}, opts)
			}
			if err != nil {
				return errors.Wrap(err, "resume change stream")
			}
		}

		err = cur.Err()
		if err != nil || cur.ID() == 0 {
			return errors.Wrap(err, "cursor")
		}

		log.Debug(ctx, "no documents yet")
	}
}
