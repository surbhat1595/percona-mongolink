package repl

import (
	"context"
	"runtime"
	"sync"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"golang.org/x/sync/errgroup"

	"github.com/percona-lab/percona-mongolink/errors"
	"github.com/percona-lab/percona-mongolink/log"
	"github.com/percona-lab/percona-mongolink/topo"
)

type collSpec struct {
	dbName  string
	spec    *mongo.CollectionSpecification
	indexes []*mongo.IndexSpecification
}

type dataCloner struct {
	Source      *mongo.Client
	Destination *mongo.Client
	Drop        bool

	specs map[string][]*collSpec

	startedAt primitive.Timestamp

	mu sync.Mutex
}

func (c *dataCloner) StartedAt() primitive.Timestamp {
	c.mu.Lock()
	rv := c.startedAt
	c.mu.Unlock()
	return rv
}

func (c *dataCloner) init(ctx context.Context) error {
	var err error
	c.startedAt, err = topo.ClusterTime(ctx, c.Source)
	if err != nil {
		return errors.Wrap(err, "get cluster time")
	}

	databases, err := c.Source.ListDatabases(ctx, bson.D{})
	if err != nil {
		return errors.Wrap(err, "list databases")
	}

	mu := sync.Mutex{}
	grp, grpCtx := errgroup.WithContext(ctx)
	grp.SetLimit(runtime.NumCPU())

	nsCatalog := make(map[string][]*collSpec)
	for _, db := range databases.Databases {
		switch db.Name {
		case "admin", "config", "local":
			continue
		}

		grp.Go(func() error {
			colls, err := c.Source.Database(db.Name).
				ListCollectionSpecifications(grpCtx, bson.D{})
			if err != nil {
				return errors.Wrap(err, "list collections")
			}

			mu.Lock()
			nsCatalog[db.Name] = make([]*collSpec, len(colls))
			mu.Unlock()

			for i, coll := range colls {
				grp.Go(func() error {
					indexes, err := c.Source.Database(db.Name).Collection(coll.Name).
						Indexes().ListSpecifications(grpCtx)
					if err != nil {
						return errors.Wrap(err, "list indexes")
					}

					mu.Lock()
					nsCatalog[db.Name][i] = &collSpec{
						dbName:  db.Name,
						spec:    coll,
						indexes: indexes,
					}
					mu.Unlock()
					return nil
				})
			}

			return nil
		})
	}

	err = grp.Wait()
	if err != nil {
		return errors.Wrap(err, "get namespaces specs")
	}

	c.mu.Lock()
	c.specs = nsCatalog
	c.mu.Unlock()

	return nil
}

func (c *dataCloner) Clone(ctx context.Context) error {
	err := c.init(ctx)
	if err != nil {
		return errors.Wrap(err, "init")
	}

	errGrp, grpCtx := errgroup.WithContext(ctx)
	errGrp.SetLimit(runtime.NumCPU())

	for _, dbSpecs := range c.specs {
		for _, spec := range dbSpecs {
			errGrp.Go(func() error {
				err := c.cloneCollection(grpCtx, spec)
				if err != nil {
					return errors.Wrapf(err, "clone %s.%s", spec.dbName, spec.spec.Name)
				}

				return nil
			})
		}
	}

	return errors.Wrap(errGrp.Wait(), "wait")
}

func (c *dataCloner) cloneCollection(ctx context.Context, spec *collSpec) error {
	log.Debug(ctx, "cloning %s.%s", spec.dbName, spec.spec.Name)

	err := c.prepareCollection(ctx, spec)
	if err != nil {
		return errors.Wrap(err, "prepare")
	}

	cur, err := c.Source.Database(spec.dbName).Collection(spec.spec.Name).
		Find(ctx, bson.D{})
	if err != nil {
		return errors.Wrap(err, "find")
	}
	defer cur.Close(ctx)

	destColl := c.Destination.Database(spec.dbName).Collection(spec.spec.Name)
	for cur.Next(ctx) {
		_, err = destColl.InsertOne(ctx, cur.Current)
		if err != nil {
			return errors.Wrap(err, "insert one")
		}
	}

	err = cur.Err()
	if err != nil {
		return errors.Wrapf(err, "cloning failed %s.%s", spec.dbName, spec.spec.Name)
	}

	log.Info(ctx, "cloned %s.%s", spec.dbName, spec.spec.Name)
	return nil
}

func (c *dataCloner) prepareCollection(ctx context.Context, spec *collSpec) error {
	if c.Drop {
		err := c.Destination.Database(spec.dbName).Collection(spec.spec.Name).Drop(ctx)
		if err != nil {
			return errors.Wrap(err, "drop")
		}
	}

	err := c.Destination.Database(spec.dbName).CreateCollection(ctx, spec.spec.Name)
	if err != nil {
		return errors.Wrap(err, "create")
	}

	return nil
}
