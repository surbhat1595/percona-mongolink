package repl

import (
	"context"
	"fmt"
	"slices"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/percona-lab/percona-mongolink/errors"
	"github.com/percona-lab/percona-mongolink/log"
)

type UnsupportedEventError struct {
	OpType OperationType
}

func (e UnsupportedEventError) Error() string {
	return "unsupported type: " + string(e.OpType)
}

func IsUnsupportedEventError(err error) bool {
	return errors.As(err, &UnsupportedEventError{})
}

type InvalidatedError struct {
	ResumeToken bson.Raw
}

func (e InvalidatedError) Error() string {
	return "invalidated"
}

func IsInvalidatedError(err error) bool {
	return errors.As(err, &InvalidatedError{})
}

type EventApplier struct {
	Client *mongo.Client

	Drop bool

	IncludeNS []string
	ExcludeNS []string
}

func (h *EventApplier) Apply(ctx context.Context, data bson.Raw) (primitive.Timestamp, error) {
	var baseEvent BaseEvent
	err := bson.Unmarshal(data, &baseEvent)
	if err != nil {
		return primitive.Timestamp{}, errors.Wrap(err, "failed to decode base event")
	}

	fullNS := baseEvent.Namespace.Database
	if baseEvent.Namespace.Collection != "" {
		fullNS += "." + baseEvent.Namespace.Collection
	}
	if len(h.IncludeNS) != 0 && !slices.Contains(h.IncludeNS, fullNS) {
		log.Debug(ctx, "apply: not included", "ns", fullNS)
		return baseEvent.ClusterTime, nil
	}
	if len(h.ExcludeNS) != 0 && slices.Contains(h.ExcludeNS, fullNS) {
		log.Debug(ctx, "apply: excluded", "ns", fullNS)
		return baseEvent.ClusterTime, nil
	}

	log.Debug(ctx, fmt.Sprintf("handling event: %s (ts: %d.%d, ns: %s)",
		baseEvent.OperationType,
		baseEvent.ClusterTime.T,
		baseEvent.ClusterTime.I,
		fullNS))

	switch baseEvent.OperationType {
	case Create:
		err = h.handleCreate(ctx, data)
	case Drop:
		err = h.handleDrop(ctx, data)
	case DropDatabase:
		err = h.handleDropDatabase(ctx, data)
	case CreateIndexes:
		err = h.handleCreateIndexes(ctx, data)
	case DropIndexes:
		err = h.handleDropIndexes(ctx, data)
	case Insert:
		err = h.handleInsert(ctx, data)
	case Delete:
		err = h.handleDelete(ctx, data)
	case Replace:
		err = h.handleReplace(ctx, data)
	case Update:
		err = h.handleUpdate(ctx, data)

	case Invalidate:
		event, err := parseEvent[InvalidateEvent](data)
		if err != nil {
			return baseEvent.ClusterTime, errors.Wrap(err, "invalidate: parse")
		}

		return baseEvent.ClusterTime, &InvalidatedError{event.ID}

	case Rename:
		fallthrough
	case Modify:
		fallthrough
	case ShardCollection:
		fallthrough
	case ReshardCollection:
		fallthrough
	case RefineCollectionShardKey:
		fallthrough

	default:
		return baseEvent.ClusterTime, &UnsupportedEventError{OpType: baseEvent.OperationType}
	}

	return baseEvent.ClusterTime, errors.Wrap(err, string(baseEvent.OperationType))
}

func (h *EventApplier) handleCreate(ctx context.Context, data bson.Raw) error {
	event, err := parseEvent[CreateEvent](data)
	if err != nil {
		return errors.Wrap(err, "parse")
	}

	if h.Drop {
		err = dropCollection(ctx,
			h.Client,
			event.Namespace.Database,
			event.Namespace.Collection)
		if err != nil {
			return errors.Wrap(err, "drop before create")
		}
	}

	if event.CollectionUUID == nil {
		return createView(ctx,
			h.Client,
			event.Namespace.Database,
			event.Namespace.Collection,
			event.OperationDescription,
		)
	}

	return createCollection(ctx,
		h.Client,
		event.Namespace.Database,
		event.Namespace.Collection,
		event.OperationDescription,
	)
}

func (h *EventApplier) handleDrop(ctx context.Context, data bson.Raw) error {
	event, err := parseEvent[DropEvent](data)
	if err != nil {
		return errors.Wrap(err, "parse")
	}

	return dropCollection(ctx, h.Client, event.Namespace.Database, event.Namespace.Collection)
}

func (h *EventApplier) handleDropDatabase(ctx context.Context, data bson.Raw) error {
	event, err := parseEvent[DropDatabaseEvent](data)
	if err != nil {
		return errors.Wrap(err, "parse")
	}

	err = h.Client.Database(event.Namespace.Database).Drop(ctx)
	return err //nolint:wrapcheck
}

func (h *EventApplier) handleCreateIndexes(ctx context.Context, data bson.Raw) error {
	event, err := parseEvent[CreateIndexesEvent](data)
	if err != nil {
		return errors.Wrap(err, "parse")
	}

	indexes := []mongo.IndexModel{}
	for _, indexSpec := range event.OperationDescription.Indexes {
		indexOptions := options.IndexOptions{
			Name:    &indexSpec.Name,
			Version: &indexSpec.Version,
			Unique:  indexSpec.Unique,
			Sparse:  indexSpec.Sparse,
		}

		indexes = append(indexes, mongo.IndexModel{
			Keys:    indexSpec.Keys,
			Options: &indexOptions,
		})
	}

	_, err = h.Client.Database(event.Namespace.Database).
		Collection(event.Namespace.Collection).
		Indexes().CreateMany(ctx, indexes)
	return err //nolint:wrapcheck
}

func (h *EventApplier) handleDropIndexes(ctx context.Context, data bson.Raw) error {
	event, err := parseEvent[DropIndexesEvent](data)
	if err != nil {
		return errors.Wrap(err, "parse")
	}

	for _, index := range event.OperationDescription.Indexes {
		// TODO: check $currentOp if the index is building
		_, err = h.Client.Database(event.Namespace.Database).
			Collection(event.Namespace.Collection).
			Indexes().DropOne(ctx, index.Name)
		if err != nil && !isIndexNotFound(err) {
			return errors.Wrapf(err, "drop %s index in %s.%s",
				index.Name,
				event.Namespace.Database,
				event.Namespace.Collection)
		}
	}

	return nil
}

func isIndexNotFound(err error) bool {
	for ; err != nil; err = errors.Unwrap(err) {
		le, ok := err.(mongo.CommandError) //nolint:errorlint
		if ok && le.Name == "IndexNotFound" {
			return true
		}
	}
	return false
}

func (h *EventApplier) handleInsert(ctx context.Context, data bson.Raw) error {
	event, err := parseEvent[InsertEvent](data)
	if err != nil {
		return errors.Wrap(err, "parse")
	}

	// use replaceOne to ensure the changed version
	_, err = h.Client.Database(event.Namespace.Database).
		Collection(event.Namespace.Collection).
		ReplaceOne(ctx, event.DocumentKey, event.FullDocument,
			options.Replace().SetUpsert(true))

	return err //nolint:wrapcheck
}

func (h *EventApplier) handleDelete(ctx context.Context, data bson.Raw) error {
	event, err := parseEvent[DeleteEvent](data)
	if err != nil {
		return errors.Wrap(err, "parse")
	}

	_, err = h.Client.Database(event.Namespace.Database).
		Collection(event.Namespace.Collection).
		DeleteOne(ctx, event.DocumentKey)
	return err //nolint:wrapcheck
}

func (h *EventApplier) handleUpdate(ctx context.Context, data bson.Raw) error {
	event, err := parseEvent[UpdateEvent](data)
	if err != nil {
		return errors.Wrap(err, "parse")
	}

	ops := bson.D{}
	if len(event.UpdateDescription.UpdatedFields) != 0 {
		ops = append(ops, bson.E{"$set", event.UpdateDescription.UpdatedFields})
	}
	if len(event.UpdateDescription.RemovedFields) != 0 {
		fields := make(bson.D, len(event.UpdateDescription.RemovedFields))
		for i, field := range event.UpdateDescription.RemovedFields {
			fields[i].Key = field
			fields[i].Value = 1
		}
		ops = append(ops, bson.E{"$unset", fields})
	}

	_, err = h.Client.Database(event.Namespace.Database).
		Collection(event.Namespace.Collection).
		UpdateOne(ctx, event.DocumentKey, ops)
	return err //nolint:wrapcheck
}

func (h *EventApplier) handleReplace(ctx context.Context, data bson.Raw) error {
	event, err := parseEvent[ReplaceEvent](data)
	if err != nil {
		return errors.Wrap(err, "parse")
	}

	_, err = h.Client.Database(event.Namespace.Database).
		Collection(event.Namespace.Collection).
		ReplaceOne(ctx, event.DocumentKey, event.FullDocument)
	return err //nolint:wrapcheck
}
