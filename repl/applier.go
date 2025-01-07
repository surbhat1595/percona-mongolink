package repl

import (
	"context"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/percona-lab/percona-mongolink/errors"
)

var ErrInvalidOpTypeField = errors.New("invalid operationType field")

type UnsupportedEventError struct {
	OpType string
}

func (e UnsupportedEventError) Error() string {
	return "unsupported type: " + e.OpType
}

type EventApplier struct {
	Client *mongo.Client
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

func (h *EventApplier) Apply(ctx context.Context, data bson.Raw) error {
	opType, ok := data.Lookup("operationType").StringValueOK()
	if !ok {
		return ErrInvalidOpTypeField
	}

	var err error
	switch opType {
	case string(Create):
		err = h.handleCreate(ctx, data)
	case string(Drop):
		err = h.handleDrop(ctx, data)
	case string(DropDatabase):
		err = h.handleDropDatabase(ctx, data)
	case string(Insert):
		err = h.handleInsert(ctx, data)
	case string(Delete):
		err = h.handleDelete(ctx, data)
	case string(Replace):
		err = h.handleReplace(ctx, data)
	case string(Update):
		err = h.handleUpdate(ctx, data)

	case string(Invalidate):
		event, err := parseEvent[InvalidateEvent](data)
		if err != nil {
			return errors.Wrap(err, "invalidate: parse")
		}

		return &InvalidatedError{event.ID}

	default:
		return &UnsupportedEventError{OpType: opType}
	}

	return errors.Wrap(err, opType)
}

func (h *EventApplier) handleCreate(ctx context.Context, data bson.Raw) error {
	event, err := parseEvent[CreateEvent](data)
	if err != nil {
		return errors.Wrap(err, "parse")
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

	err = h.Client.Database(event.Namespace.Database).
		Collection(event.Namespace.Collection).Drop(ctx)
	return err //nolint:wrapcheck
}

func (h *EventApplier) handleDropDatabase(ctx context.Context, data bson.Raw) error {
	event, err := parseEvent[DropDatabaseEvent](data)
	if err != nil {
		return errors.Wrap(err, "parse")
	}

	err = h.Client.Database(event.Namespace.Database).Drop(ctx)
	return err //nolint:wrapcheck
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
