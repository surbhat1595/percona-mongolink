package repl

import (
	"context"
	"strings"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"

	"github.com/percona-lab/percona-mongolink/errors"
	"github.com/percona-lab/percona-mongolink/log"
)

type MissedFieldError struct {
	Name string
}

func (e MissedFieldError) Error() string {
	return "missed " + e.Name + " field"
}

type TimeseriesError struct {
	NS Namespace
}

func (e TimeseriesError) Error() string {
	return "unsupported timeseries: " + e.NS.Database + "." + e.NS.Collection
}

func createView(
	ctx context.Context,
	m *mongo.Client,
	dbName string,
	collName string,
	options bson.D,
) error {
	var viewOn string
	var pipeline bson.A
	for _, e := range options {
		var ok bool
		switch e.Key {
		case "viewOn":
			viewOn, ok = e.Value.(string)
			if !ok {
				return MissedFieldError{"viewOn"}
			}
		case "pipeline":
			pipeline, ok = e.Value.(bson.A)
			if !ok {
				return MissedFieldError{"pipeline"}
			}
		default:
			log.Debug(ctx, "HandleCreate: unknown field", "ns", dbName+"."+collName)
		}
	}

	if strings.HasPrefix(viewOn, "system.buckets.") {
		return TimeseriesError{
			NS: Namespace{
				Database:   dbName,
				Collection: collName,
			},
		}
	}

	err := m.Database(dbName).CreateView(ctx, collName, viewOn, pipeline)
	return errors.Wrap(err, "create view")
}

func createCollection(
	ctx context.Context,
	m *mongo.Client,
	dbName string,
	collName string,
	options bson.D,
) error {
	var opts bson.D
	for _, e := range options {
		switch e.Key {
		case "idIndex":
			idIndex, ok := e.Value.(bson.D)
			if !ok {
				return MissedFieldError{"idIndex"}
			}
			opts = append(opts, bson.E{"idIndex", idIndex})
		case "clusteredIndex":
			clusteredIndex, ok := e.Value.(bson.D)
			if !ok {
				return MissedFieldError{"clusteredIndex"}
			}
			opts = append(opts, bson.E{"clusteredIndex", clusteredIndex})
		default:
			log.Debug(ctx, "HandleCreate: unknown field", "ns", dbName+"."+collName)
		}
	}

	cmd := append(bson.D{{"create", collName}}, opts...)
	res := m.Database(dbName).RunCommand(ctx, cmd)
	return errors.Wrap(res.Err(), "create collection")
}
