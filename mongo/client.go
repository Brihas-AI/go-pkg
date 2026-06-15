package mongo

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Brihas-AI/go-pkg/env"
	"github.com/Brihas-AI/go-pkg/utils"
	_ "github.com/Brihas-AI/go-pkg/utils"
	log "github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/bson/primitive"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/gridfs"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"

	"go.elastic.co/apm/v2"
)

var (
	mongoClient *Client
	once        sync.Once
)

type Client struct {
	Client   *mongo.Client
	Database *mongo.Database
	Bucket   *gridfs.Bucket
	ready    atomic.Bool
}

func InitMongo() {
	once.Do(func() {
		client, err := NewMongoClient()
		if err != nil {
			log.WithFields(log.Fields{
				"error":  err.Error(),
				"source": "mongo.InitMongo",
			}).Fatal("[Mongo] init failed")
		}
		mongoClient = client
		fmt.Println("[Mongo] client initialized successfully")
	})
}

func GetClient() *Client {
	return mongoClient
}

func (c *Client) IsReady() bool {
	return c.ready.Load()
}

func (c *Client) GetDatabase() *mongo.Database {
	return c.Database
}

func NewMongoClient() (*Client, error) {
	requiredEnv := []string{"MONGO_HOST", "MONGO_PORT", "MONGO_DB"}
	var missing []string
	for _, key := range requiredEnv {
		if os.Getenv(key) == "" {
			missing = append(missing, key)
		}
	}
	if len(missing) > 0 {
		log.WithFields(log.Fields{
			"missing": missing,
			"source":  "mongo.NewMongoClient",
		}).Error("[Mongo] required env vars are missing")

		return nil, fmt.Errorf("missing required env vars: %v", missing)
	}

	var uri string
	username := os.Getenv("MONGO_USERNAME")
	password := os.Getenv("MONGO_PASSWORD")
	if username != "" && password != "" {
		uri = fmt.Sprintf(
			"mongodb://%s:%s@%s:%s/%s",
			username,
			password,
			os.Getenv("MONGO_HOST"),
			os.Getenv("MONGO_PORT"),
			os.Getenv("MONGO_DB"),
		)
	} else {
		log.Warn("[Mongo] MONGO_USERNAME/MONGO_PASSWORD not set — connecting without auth")
		uri = fmt.Sprintf(
			"mongodb://%s:%s/%s",
			os.Getenv("MONGO_HOST"),
			os.Getenv("MONGO_PORT"),
			os.Getenv("MONGO_DB"),
		)
	}

	mongoParams := strings.TrimSpace(os.Getenv("MONGO_PARAMS"))
	if mongoParams != "" {
		uri += "?" + mongoParams
	}

	opts := options.Client().ApplyURI(uri).
		SetMinPoolSize(uint64(env.GetEnvOrDefaultInt("MONGO_MIN_POOL_SIZE", 1))).
		SetMaxPoolSize(uint64(env.GetEnvOrDefaultInt("MONGO_MAX_POOL_SIZE", 1))).
		SetMaxConnIdleTime(10 * time.Minute).
		SetConnectTimeout(5 * time.Second)

	client := &Client{}
	client.connectWithRetry(opts, os.Getenv("MONGO_DB"))
	return client, nil
}

func (c *Client) connectWithRetry(opts *options.ClientOptions, dbName string) {
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		client, err := mongo.Connect(ctx, opts)
		cancel()
		if err != nil {
			log.WithFields(log.Fields{
				"error":  err.Error(),
				"source": "mongo.connectWithRetry",
				"db":     dbName,
			}).Error("[Mongo] connect failed. Retrying after 5 seconds...")

			time.Sleep(5 * time.Second)
			continue
		}

		ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
		err = client.Ping(ctx, getReadPref())
		cancel()
		if err != nil {
			log.WithFields(log.Fields{
				"error":  err.Error(),
				"source": "mongo.connectWithRetry",
				"db":     dbName,
			}).Error("[Mongo] ping failed. Disconnecting and retrying after 3 seconds...")

			err = client.Disconnect(context.Background())
			if err != nil {
				log.WithFields(log.Fields{
					"error":  err.Error(),
					"source": "mongo.connectWithRetry",
					"db":     dbName,
				}).Warn("[Mongo] disconnect failed after ping failure")
			}

			time.Sleep(3 * time.Second)
			continue
		}

		c.Client = client
		c.Database = client.Database(dbName)
		bucket, err := gridfs.NewBucket(c.Database)
		if err != nil {
			log.WithFields(log.Fields{
				"error":  err.Error(),
				"source": "mongo.connectWithRetry",
				"db":     dbName,
			}).Fatal("[Mongo] failed to initialize GridFS bucket")
		}
		c.Bucket = bucket
		c.ready.Store(true)
		log.Println("[Mongo] connection established.")
		return
	}
}

func getReadPref() *readpref.ReadPref {
	switch strings.ToLower(os.Getenv("MONGO_READ_PREFERENCE")) {
	case "primary":
		return readpref.Primary()
	case "primarypreferred":
		return readpref.PrimaryPreferred()
	case "secondary":
		return readpref.Secondary()
	case "secondarypreferred":
		return readpref.SecondaryPreferred()
	case "nearest":
		return readpref.Nearest()
	default:
		return readpref.Primary()
	}
}

func (c *Client) GetCollection(name string) *mongo.Collection {
	if c.Database == nil {
		log.WithFields(log.Fields{
			"source":     "mongo.GetCollection",
			"collection": name,
		}).Error("[Mongo] database is not initialized")
		return nil
	}
	return c.Database.Collection(name)
}

type IndexConfig struct {
	TTLSeconds int32
	Indexes    []mongo.IndexModel
}

func (c *Client) SetupCollectionIndexes(configs map[string]IndexConfig) {
	for collectionName, cfg := range configs {
		if cfg.TTLSeconds > 0 {
			if err := c.ApplyTTLIndex(collectionName, cfg.TTLSeconds); err != nil {
				log.WithFields(log.Fields{
					"error":       err.Error(),
					"collection":  collectionName,
					"ttl_seconds": cfg.TTLSeconds,
					"source":      "mongo.SetupCollectionIndexes",
				}).Error("[Mongo] failed to create TTL index")
			}
		}
		if len(cfg.Indexes) > 0 {
			if err := c.ApplyCompoundIndexes(collectionName, cfg.Indexes); err != nil {
				log.WithFields(log.Fields{
					"error":      err.Error(),
					"collection": collectionName,
					"indices":    cfg.Indexes,
					"source":     "mongo.SetupCollectionIndexes",
				}).Warn("[Mongo] failed to create compound indexes")
			}
		}
	}
}

func (c *Client) ApplyTTLIndex(collectionName string, ttlSeconds int32) error {
	if ttlSeconds == 0 {
		return nil
	}
	if ttlSeconds < 0 {
		err := errors.New("TTL seconds can't be negative")
		log.WithFields(log.Fields{
			"collection":  collectionName,
			"ttl_seconds": ttlSeconds,
			"source":      "mongo.ApplyTTLIndex",
		}).Error("[Mongo] TTL seconds can't be negative")
		return err
	}

	col, err := c.requireReadyCollection(collectionName, "mongo.ApplyTTLIndex")
	if err != nil {
		return err
	}

	indexName := "created_at_ttl"
	existingIndexes, err := col.Indexes().List(context.Background())
	if err != nil {
		log.WithFields(log.Fields{
			"error":      err.Error(),
			"collection": collectionName,
			"source":     "mongo.ApplyTTLIndex",
		}).Error("[Mongo] failed to list indexes")
		return err
	}

	ttlIndexExists := false
	var existingTTL int32
	for existingIndexes.Next(context.Background()) {
		var indexDoc bson.M
		if err = existingIndexes.Decode(&indexDoc); err != nil {
			log.WithFields(log.Fields{
				"error":      err.Error(),
				"collection": collectionName,
				"source":     "mongo.ApplyTTLIndex",
			}).Warn("[Mongo] failed to decode index metadata")
			continue
		}
		if indexDoc["name"] == indexName {
			ttlIndexExists = true
			if opts, ok := indexDoc["expireAfterSeconds"].(int32); ok {
				existingTTL = opts
			} else if optsF, ok := indexDoc["expireAfterSeconds"].(float64); ok {
				existingTTL = int32(optsF)
			}
			break
		}
	}

	// Drop and recreate TTL index if TTL differs
	if ttlIndexExists && existingTTL != ttlSeconds {
		if _, err = col.Indexes().DropOne(context.Background(), indexName); err != nil {
			log.WithFields(log.Fields{
				"error":      err.Error(),
				"collection": collectionName,
				"old_ttl":    existingTTL,
				"new_ttl":    ttlSeconds,
			}).Error("[Mongo] failed to drop existing TTL index")
			return err
		}
		log.WithFields(log.Fields{
			"collection": collectionName,
			"old_ttl":    existingTTL,
			"new_ttl":    ttlSeconds,
		}).Info("[Mongo] dropped existing TTL index to update TTL")
	}

	// Only create an index if it doesn't exist or TTL has changed
	if !ttlIndexExists || existingTTL != ttlSeconds {
		indexModel := mongo.IndexModel{
			Keys:    bson.D{{Key: "created_at", Value: 1}},
			Options: options.Index().SetExpireAfterSeconds(ttlSeconds).SetName(indexName),
		}
		_, err = col.Indexes().CreateOne(context.Background(), indexModel)
		if err != nil {
			log.WithFields(log.Fields{
				"error":       err.Error(),
				"collection":  collectionName,
				"ttl_seconds": ttlSeconds,
				"source":      "mongo.ApplyTTLIndex",
			}).Error("[Mongo] failed to create TTL index")
			return err
		}
		log.WithFields(log.Fields{
			"collection":  collectionName,
			"ttl_seconds": ttlSeconds,
		}).Info("[Mongo] TTL index is created")
	} else {
		log.WithFields(log.Fields{
			"collection":  collectionName,
			"ttl_seconds": ttlSeconds,
		}).Info("[Mongo] TTL index already exists with correct TTL")
	}
	return nil
}

func (c *Client) ApplyCompoundIndexes(collectionName string, indexes []mongo.IndexModel) error {
	col, err := c.requireReadyCollection(collectionName, "mongo.ApplyCompoundIndexes")
	if err != nil {
		return err
	}

	_, err = col.Indexes().CreateMany(context.Background(), indexes)
	if err != nil {
		log.WithFields(log.Fields{
			"error":      err.Error(),
			"collection": collectionName,
			"source":     "mongo.ApplyCompoundIndexes",
		}).Error("[Mongo] failed to create compound indexes")
		return err
	}

	log.WithFields(log.Fields{
		"collection":  collectionName,
		"index_count": len(indexes),
	}).Info("[Mongo] compound indexes ensured")
	return nil
}

func (c *Client) InsertOne(ctx context.Context, collectionName string, doc any) (string, error) {
	if doc == nil {
		log.WithFields(log.Fields{
			"collection": collectionName,
			"source":     "mongo.InsertOne",
		}).Error("[Mongo] Document is nil")
		return "", errors.New("document is nil")
	}

	col, err := c.requireReadyCollection(collectionName, "mongo.InsertOne")
	if err != nil {
		return "", err
	}

	span, _ := apm.StartSpan(ctx, "mongo.InsertOne "+collectionName, "db.mongodb")
	defer span.End()

	res, err := col.InsertOne(ctx, doc)

	if err != nil {
		log.WithFields(log.Fields{
			"error":      err.Error(),
			"collection": collectionName,
			"source":     "mongo.InsertOne",
		}).Error("[Mongo] Insert failed")
		return "", fmt.Errorf("insert failed: %w", err)
	}
	return utils.ToString(res.InsertedID), nil
}

func (c *Client) UpdateByID(collectionName string, docID string, update any) error {
	if docID == "" {
		log.WithFields(log.Fields{
			"source": "mongo.UpdateByID",
		}).Error("[Mongo] document ID is empty")
	}

	if update == nil {
		return nil
	}

	col, err := c.requireReadyCollection(collectionName, "mongo.UpdateByID")
	if err != nil {
		return err
	}

	objectID, err := primitive.ObjectIDFromHex(docID)
	if err != nil {
		log.WithFields(log.Fields{
			"error":  err.Error(),
			"source": "mongo.UpdateByID",
			"docID":  docID,
		}).Error("[Mongo] invalid object ID format")
		return fmt.Errorf("invalid ObjectID format: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var filter bson.M
	if objectID, err = primitive.ObjectIDFromHex(docID); err == nil {
		filter = bson.M{"_id": objectID}
	} else {
		filter = bson.M{"_id": docID}
	}

	updateWrapper := bson.M{"$set": update}

	span, _ := apm.StartSpan(ctx, "mongo.UpdateByID "+collectionName, "db.mongodb")
	defer span.End()

	res, err := col.UpdateOne(ctx, filter, updateWrapper)
	if err != nil {
		log.WithFields(log.Fields{
			"error":  err.Error(),
			"source": "mongo.UpdateByID",
			"docID":  docID,
		}).Error("[Mongo] update failed by document ID")
		return fmt.Errorf("failed to update document with ID %s: %w", docID, err)
	}

	if res.MatchedCount == 0 {
		log.WithFields(log.Fields{
			"docID":  docID,
			"source": "mongo.UpdateByID",
		}).Warn("[Mongo] no document is matched with the given ID")
	}

	return nil
}

func (c *Client) FindLatestOne(collectionName string, sortField string, filter any, out any) error {
	col, err := c.requireReadyCollection(collectionName, "mongo.FindLatestOne")
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	span, _ := apm.StartSpan(ctx, "mongo.FindLatestOne "+collectionName, "db.mongodb")
	defer span.End()

	opts := options.FindOne().SetSort(bson.D{{Key: sortField, Value: -1}})
	err = col.FindOne(ctx, filter, opts).Decode(out)
	if errors.Is(err, mongo.ErrNoDocuments) {
		log.WithFields(log.Fields{
			"error":  err.Error(),
			"source": "mongo.FindLatestOne",
			"filter": filter,
		}).Error("[Mongo] no documents are found")
		return nil
	}
	if err != nil {
		log.WithFields(log.Fields{
			"error":  err.Error(),
			"source": "mongo.FindLatestOne",
			"filter": filter,
		}).Error("[Mongo] find latest one failed")
		return fmt.Errorf("find latest one failed: %w", err)
	}
	return nil
}

func (c *Client) GetGridFSFile(fileID interface{}) ([]byte, error) {
	stream, err := c.Bucket.OpenDownloadStream(fileID)
	if err != nil {
		log.WithFields(log.Fields{
			"error":   err.Error(),
			"file_id": fileID,
			"source":  "mongo.GetGridFSFile",
		}).Error("[Mongo] failed to open GridFS stream")
		return nil, fmt.Errorf("failed to open GridFS stream: %w", err)
	}

	defer func() {
		if errC := stream.Close(); errC != nil {
			log.WithFields(log.Fields{
				"error":   errC.Error(),
				"file_id": fileID,
				"source":  "mongo.GetGridFSFile",
			}).Error("[Mongo] failed to close GridFS stream")
		}
	}()

	data, errR := io.ReadAll(stream)
	if errR != nil {
		log.WithFields(log.Fields{
			"error":   errR.Error(),
			"file_id": fileID,
			"source":  "mongo.GetGridFSFile",
		}).Error("[Mongo] failed to read GridFS content")
		return nil, fmt.Errorf("failed to read GridFS content: %w", err)
	}

	return data, nil
}

func (c *Client) uploadToGridFS(data []byte, filename string) (interface{}, error) {
	stream, err := c.Bucket.OpenUploadStream(filename)
	if err != nil {
		return nil, fmt.Errorf("upload stream failed: %w", err)
	}
	defer func(stream *gridfs.UploadStream) {
		err = stream.Close()
		if err != nil {
			log.WithFields(log.Fields{
				"error":  err.Error(),
				"source": "mongo.uploadToGridFS",
			}).Error("[Mongo] failed to close upload stream")
			return
		}
	}(stream)

	if _, err = stream.Write(data); err != nil {
		log.WithFields(log.Fields{
			"error":  err.Error(),
			"source": "mongo.uploadToGridFS",
		}).Error("[Mongo] failed to write to upload stream")
		return nil, fmt.Errorf("write to GridFS failed: %w", err)
	}
	return stream.FileID, nil
}

func (c *Client) InsertWithGridFS(ctx context.Context, payload []byte, collectionName string, doc bson.M) (string, error) {
	_, err := c.requireReadyCollection(collectionName, "mongo.InsertWithGridFS")
	if err != nil {
		return "", err
	}

	span, _ := apm.StartSpan(ctx, "mongo.InsertWithGridFS "+collectionName, "db.mongodb")
	defer span.End()

	fileID, err := c.uploadToGridFS(payload, "")
	if err != nil {
		log.WithFields(log.Fields{
			"error":  err.Error(),
			"source": "mongo.InsertWithGridFS",
			"doc":    doc,
		}).Error("[Mongo] failed to upload to GridFS")
		return "", fmt.Errorf("failed to upload to GridFS: %w", err)
	}

	doc["data_ref"] = fileID
	doc["created_at"] = time.Now().UTC()

	objectID, err := c.InsertOne(ctx, collectionName, doc)
	if err != nil {
		log.WithFields(log.Fields{
			"error":  err.Error(),
			"source": "mongo.InsertWithGridFS",
			"doc":    doc,
		}).Error("[Mongo] failed to insert to DB")
		return "", err
	}

	return objectID, nil
}

func (c *Client) Disconnect(ctx context.Context) error {
	if c.Client != nil {
		if err := c.Client.Disconnect(ctx); err != nil {
			log.WithFields(log.Fields{
				"error":  err.Error(),
				"source": "mongo.Disconnect",
			}).Error("[Mongo] disconnect failed")
			return err
		}
		c.Client = nil
		c.Database = nil
		c.Bucket = nil
	}
	return nil
}

func (c *Client) HealthCheck() error {
	if !c.IsReady() {
		log.WithFields(log.Fields{
			"source": "mongo.HealthCheck",
		}).Error("[Mongo] mongo client is not ready")
		return fmt.Errorf("mongo not ready")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	return c.Client.Ping(ctx, readpref.Primary())
}

func (c *Client) requireReadyCollection(collectionName, caller string) (*mongo.Collection, error) {
	if collectionName == "" {
		log.WithFields(log.Fields{
			"source": caller,
		}).Error("[Mongo] collection name is empty")
		return nil, errors.New("collection name is empty")
	}

	if !c.IsReady() {
		log.WithFields(log.Fields{
			"collection": collectionName,
			"source":     caller,
		}).Error("[Mongo] mongo client is not ready")
		return nil, errors.New("mongo client is not ready")
	}

	col := c.GetCollection(collectionName)
	if col == nil {
		log.WithFields(log.Fields{
			"collection": collectionName,
			"source":     caller,
		}).Error("[Mongo] collection is not found")
		return nil, errors.New("collection not found")
	}

	return col, nil
}
