package repl

import (
	"context"
	"strings"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/percona-lab/percona-mongolink/errors"
)

type InvalidFieldError struct {
	Name string
}

func (e InvalidFieldError) Error() string {
	return "invalid field: " + e.Name
}

type TimeseriesError struct {
	NS Namespace
}

func (e TimeseriesError) Error() string {
	return "unsupported timeseries: " + e.NS.String()
}

func createView(
	ctx context.Context,
	m *mongo.Client,
	dbName string,
	viewName string,
	opts *createEventOptions,
) error {
	if strings.HasPrefix(opts.ViewOn, "system.buckets.") {
		return TimeseriesError{
			NS: Namespace{
				Database:   dbName,
				Collection: viewName,
			},
		}
	}

	err := m.Database(dbName).CreateView(ctx,
		viewName,
		opts.ViewOn,
		opts.Pipeline,
		options.CreateView().SetCollation(opts.Collation))
	return errors.Wrap(err, "create view")
}

func createCollection(
	ctx context.Context,
	m *mongo.Client,
	dbName string,
	collName string,
	opts *createEventOptions,
) error {
	cmd := bson.D{{"create", collName}}
	if opts.ClusteredIndex != nil {
		cmd = append(cmd, bson.E{"clusteredIndex", opts.ClusteredIndex})
	} else {
		cmd = append(cmd, bson.E{"idIndex", opts.IDIndex})
	}

	if opts.Capped {
		cmd = append(cmd, bson.E{"capped", opts.Capped})
		if opts.Size != 0 {
			cmd = append(cmd, bson.E{"size", opts.Size})
		}
		if opts.Max != 0 {
			cmd = append(cmd, bson.E{"max", opts.Max})
		}
	}

	if opts.Collation != nil {
		cmd = append(cmd, bson.E{"collation", opts.Collation.ToDocument()}) //nolint:staticcheck
	}

	res := m.Database(dbName).RunCommand(ctx, cmd)
	return errors.Wrap(res.Err(), "create collection")
}

func dropCollection(ctx context.Context, m *mongo.Client, dbName, collName string) error {
	err := m.Database(dbName).Collection(collName).Drop(ctx)
	return errors.Wrap(err, "drop collection")
}
