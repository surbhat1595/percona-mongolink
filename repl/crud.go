package repl

import (
	"context"
	"strings"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"

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
	collName string,
	options *createEventOptions,
) error {
	if strings.HasPrefix(options.ViewOn, "system.buckets.") {
		return TimeseriesError{
			NS: Namespace{
				Database:   dbName,
				Collection: collName,
			},
		}
	}

	err := m.Database(dbName).CreateView(ctx, collName, options.ViewOn, options.Pipeline)
	return errors.Wrap(err, "create view")
}

func createCollection(
	ctx context.Context,
	m *mongo.Client,
	dbName string,
	collName string,
	options *createEventOptions,
) error {
	var opts bson.D
	if options.ClusteredIndex != nil {
		opts = append(opts, bson.E{"clusteredIndex", options.ClusteredIndex})
	} else {
		opts = append(opts, bson.E{"idIndex", options.IDIndex})
	}

	if options.Capped {
		opts = append(opts, bson.E{"capped", options.Capped})
		if options.Size != 0 {
			opts = append(opts, bson.E{"size", options.Size})
		}
		if options.Max != 0 {
			opts = append(opts, bson.E{"max", options.Max})
		}
	}

	cmd := append(bson.D{{"create", collName}}, opts...)
	res := m.Database(dbName).RunCommand(ctx, cmd)
	return errors.Wrap(res.Err(), "create collection")
}

func dropCollection(ctx context.Context, m *mongo.Client, dbName, collName string) error {
	err := m.Database(dbName).Collection(collName).Drop(ctx)
	return errors.Wrap(err, "drop collection")
}
