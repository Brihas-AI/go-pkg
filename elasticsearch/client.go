package elasticsearch

import (
	"github.com/Brihas-AI/go-pkg/env"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/elastic/go-elasticsearch/v8/esapi"
	log "github.com/sirupsen/logrus"
	"io"
	"net"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/elastic/go-elasticsearch/v8"
)

var (
	esClient *Client
	once     sync.Once
)

type Client struct {
	es        *elasticsearch.Client
	Transport *http.Transport
}

func InitElasticsearch() {
	once.Do(func() {
		client, err := NewElasticSearch()
		if err != nil {
			log.WithError(err).Fatal("[Elasticsearch] failed to initialize")
		}
		esClient = client
		fmt.Println("[Elasticsearch] initialized successfully")
	})
}

func NewElasticSearch() (*Client, error) {
	transport := &http.Transport{
		MaxIdleConns:        100,
		MaxIdleConnsPerHost: env.GetEnvOrDefaultInt("ES_MAX_IDLE_CONNS_PER_HOST", 10),
		MaxConnsPerHost:     env.GetEnvOrDefaultInt("ES_MAX_CONNS_PER_HOST", 100),
		IdleConnTimeout:     time.Duration(env.GetEnvOrDefaultInt("ES_IDLE_CONN_TIMEOUT", 90)) * time.Second,
		DialContext: (&net.Dialer{
			Timeout:   time.Duration(env.GetEnvOrDefaultInt("ES_DIAL_TIMEOUT", 5)) * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
	}

	elasticURL := os.Getenv("ELASTIC_SEARCH_URL")
	if elasticURL == "" {
		return nil, fmt.Errorf("ELASTIC_SEARCH_URL is empty")
	}

	es, err := elasticsearch.NewClient(elasticsearch.Config{
		Addresses: []string{elasticURL},
		Transport: transport,
	})
	if err != nil {
		log.WithFields(log.Fields{
			"error":  err.Error(),
			"url":    elasticURL,
			"source": "elasticsearch.NewElasticSearch",
		}).Error("[Elasticsearch] failed to create elasticsearch client")
		return nil, err
	}
	esClient = &Client{
		es:        es,
		Transport: transport,
	}
	return esClient, nil
}

func GetElasticSearchClient() *Client {
	return esClient
}

func (c *Client) Close() {
	c.Transport.CloseIdleConnections()
}

func (c *Client) AddDocumentsToIndexWithRetry(indexName string, documents []interface{}, chunkSize, maxRetries int) error {
	var res *esapi.Response
	var err error

	// Process documents in chunks
	chunkSize *= 2
	for start := 0; start < len(documents); start += chunkSize {
		end := start + chunkSize
		if end > len(documents) {
			end = len(documents)
		}
		chunk := documents[start:end]

		// Create a buffer to accumulate the bulk request
		var buf bytes.Buffer

		// Prepare bulk request by iterating in pairs (action, document)
		for i := 0; i < len(chunk); i += 2 {
			if i+1 >= len(chunk) {
				log.WithFields(log.Fields{
					"index":  indexName,
					"source": "elasticsearch.AddDocumentsToIndexWithRetry",
				}).Error("[Elasticsearch] missing document for action")
				return fmt.Errorf("missing document for action at index %d", i)
			}

			// The action is already part of the input (at even indices)
			action := chunk[i]

			// Document is the next element in the array (at odd indices)
			document := chunk[i+1]

			// Encode both action and document to the buffer
			enc := json.NewEncoder(&buf)
			if errE := enc.Encode(action); errE != nil {
				log.WithFields(log.Fields{
					"error":  errE.Error(),
					"index":  indexName,
					"source": "elasticsearch.AddDocumentsToIndexWithRetry",
				}).Error("[Elasticsearch] failed to encode action")
				return fmt.Errorf("encode action failed: %w", errE)
			}

			if errE := enc.Encode(document); errE != nil {
				log.WithFields(log.Fields{
					"error":  errE.Error(),
					"index":  indexName,
					"source": "elasticsearch.AddDocumentsToIndexWithRetry",
				}).Error("[Elasticsearch] failed to encode document")
				return fmt.Errorf("encode document failed: %w", errE)
			}
		}

		// Retry mechanism: attempt to perform the bulk insert
		for attempt := 1; attempt <= maxRetries; attempt++ {
			res, err = c.es.Bulk(&buf)
			if err != nil {
				log.WithFields(log.Fields{
					"error":  err.Error(),
					"index":  indexName,
					"source": "elasticsearch.AddDocumentsToIndexWithRetry",
				}).Error("[Elasticsearch] failed to add documents to index")
				return fmt.Errorf("bulk insert failed: %w", err)
			}

			// If the request resulted in an error, retry
			if res.IsError() {
				body, _ := io.ReadAll(res.Body)
				log.WithFields(log.Fields{
					"status": res.Status(),
					"index":  indexName,
					"source": "elasticsearch.AddDocumentsToIndexWithRetry",
				}).Error("[Elasticsearch] bulk insert HTTP error")

				// Retry for transient errors like 429 (Too Many Requests) or 500 (Server Errors)
				if res.Status() == "429" || res.Status() == "500" {
					log.Warnf("Retrying due to transient error, attempt #%d", attempt)
					time.Sleep(time.Duration(attempt) * time.Second)
					continue
				}
				return fmt.Errorf("bulk insert error: %s", body)
			}
			break
		}

		if res == nil {
			log.Warn("[Elasticsearch] response is nil")
			return fmt.Errorf("[Elasticsearch] response is nil")
		}

		// Parse the response
		var response struct {
			Errors bool `json:"errors"`
			Items  []struct {
				Index struct {
					Status int                    `json:"status"`
					Error  map[string]interface{} `json:"error,omitempty"`
					ID     string                 `json:"_id"`
				} `json:"index"`
			} `json:"items"`
		}
		if err = json.NewDecoder(res.Body).Decode(&response); err != nil {
			log.WithFields(log.Fields{
				"error":  err.Error(),
				"index":  indexName,
				"source": "elasticsearch.AddDocumentsToIndexWithRetry",
			}).Error("[Elasticsearch] failed to decode bulk insert response")
			return fmt.Errorf("failed to parse bulk response: %w", err)
		}

		// Handle indexing errors for individual documents
		for _, item := range response.Items {
			if item.Index.Status >= 400 {
				log.WithFields(log.Fields{
					"error":  item.Index.Error,
					"id":     item.Index.ID,
					"index":  indexName,
					"source": "elasticsearch.AddDocumentsToIndexWithRetry",
				}).Error("[Elasticsearch] failed to index document")
			}
		}

		closeBodyErr := res.Body.Close()
		if closeBodyErr != nil {
			log.WithFields(log.Fields{
				"error":  closeBodyErr.Error(),
				"index":  indexName,
				"source": "elasticsearch.AddDocumentsToIndexWithRetry",
			}).Error("[Elasticsearch] failed to close body")
		}
	}

	return nil
}

func (c *Client) SearchWithQuery(indexName string, searchQuery map[string]interface{}, size int) (map[string]interface{}, error) {
	if size <= 0 {
		size = 1
	}

	if _, exists := searchQuery["size"]; !exists {
		searchQuery["size"] = size
	}

	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(searchQuery); err != nil {
		log.WithFields(log.Fields{
			"error":  err.Error(),
			"index":  indexName,
			"source": "elasticsearch.SearchWithQuery",
		}).Error("[Elasticsearch] failed to encode search body")
		return nil, fmt.Errorf("failed to encode search body: %w", err)
	}

	res, err := c.es.Search(
		c.es.Search.WithIndex(indexName),
		c.es.Search.WithBody(&buf),
		c.es.Search.WithTrackTotalHits(true),
		c.es.Search.WithPretty(),
	)

	if err != nil {
		log.WithFields(log.Fields{
			"error":  err.Error(),
			"index":  indexName,
			"source": "elasticsearch.SearchWithQuery",
		}).Error("[Elasticsearch] failed search request")
		return nil, fmt.Errorf("elasticsearch search failed: %w", err)
	}

	if res.Body != nil {
		defer func(Body io.ReadCloser) {
			closeBodyErr := Body.Close()
			if closeBodyErr != nil {
				log.WithFields(log.Fields{
					"error":  closeBodyErr.Error(),
					"index":  indexName,
					"source": "elasticsearch.SearchWithQuery",
				}).Error("[Elasticsearch] failed to close bulk response body")
			}
		}(res.Body)
	}

	if res.IsError() {
		bodyBytes, _ := io.ReadAll(res.Body)
		log.WithFields(log.Fields{
			"status": res.Status(),
			"index":  indexName,
			"body":   string(bodyBytes),
			"source": "elasticsearch.SearchWithQuery",
		}).Error("[Elasticsearch] search request returned error")
		return nil, fmt.Errorf("search error: %s", string(bodyBytes))
	}

	var response map[string]interface{}
	if err = json.NewDecoder(res.Body).Decode(&response); err != nil {
		log.WithFields(log.Fields{
			"error":  err.Error(),
			"index":  indexName,
			"source": "elasticsearch.SearchWithQuery",
		}).Error("[Elasticsearch] failed to decode search response")
		return nil, fmt.Errorf("failed to decode search response: %w", err)
	}

	return response, nil
}

func (c *Client) DeleteIndex(indexName string) error {
	exists, err := c.IndexExists(indexName)
	if err != nil {
		log.WithFields(log.Fields{
			"error":  err.Error(),
			"index":  indexName,
			"source": "elasticsearch.DeleteIndex",
		}).Error("[Elasticsearch] failed to check existence of index")
		return fmt.Errorf("error checking index existence: %w", err)
	}

	if !exists {
		log.WithFields(log.Fields{
			"index":  indexName,
			"source": "elasticsearch.DeleteIndex",
		}).Error("[Elasticsearch] index does not exist")
		return fmt.Errorf("index %s does not exist", indexName)
	}

	// Delete the index
	res, err := c.es.Indices.Delete([]string{indexName})
	if err != nil {
		log.WithFields(log.Fields{
			"error":  err.Error(),
			"index":  indexName,
			"source": "elasticsearch.DeleteIndex",
		}).Error("[Elasticsearch] failed to delete index")
		return fmt.Errorf("failed to delete index: %w", err)
	}

	if res.Body != nil {
		defer func(Body io.ReadCloser) {
			closeBodyErr := Body.Close()
			if closeBodyErr != nil {
				log.WithFields(log.Fields{
					"error":  closeBodyErr.Error(),
					"index":  indexName,
					"source": "elasticsearch.DeleteIndex",
				}).Error("[ElasticSearch] failed to close bulk response body")
			}
		}(res.Body)
	}

	if res.IsError() {
		bodyBytes, _ := io.ReadAll(res.Body)
		log.WithFields(log.Fields{
			"status": res.Status(),
			"index":  indexName,
			"body":   string(bodyBytes),
			"source": "elasticsearch.DeleteIndex",
		}).Error("[Elasticsearch] failed to delete index")
		return fmt.Errorf("delete index error: %s", string(bodyBytes))
	}

	var respBody map[string]interface{}
	if err = json.NewDecoder(res.Body).Decode(&respBody); err != nil {
		log.WithFields(log.Fields{
			"error":  err.Error(),
			"index":  indexName,
			"source": "elasticsearch.DeleteIndex",
		}).Error("[Elasticsearch] failed to parse delete index response")
		return fmt.Errorf("failed to parse delete index response: %w", err)
	}

	if ack, ok := respBody["acknowledged"].(bool); ok && ack {
		log.WithFields(log.Fields{
			"index":  indexName,
			"source": "elasticsearch.DeleteIndex",
		}).Info("[Elasticsearch] index deleted successfully")
		return nil
	}

	log.WithFields(log.Fields{
		"index":  indexName,
		"source": "elasticsearch.DeleteIndex",
	}).Error("[Elasticsearch] index deletion not acknowledged")
	return fmt.Errorf("index deletion not acknowledged")
}

func (c *Client) CountDocuments(indexName string, query map[string]interface{}) (int64, error) {
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(query); err != nil {
		log.WithFields(log.Fields{
			"error":  err.Error(),
			"index":  indexName,
			"source": "elasticsearch.CountDocuments",
		}).Error("[Elasticsearch] failed to encode count query")
		return 0, fmt.Errorf("failed to encode count query: %w", err)
	}

	res, err := c.es.Count(
		c.es.Count.WithIndex(indexName),
		c.es.Count.WithBody(&buf),
	)

	if err != nil {
		log.WithFields(log.Fields{
			"error":  err.Error(),
			"index":  indexName,
			"source": "elasticsearch.CountDocuments",
		}).Error("[Elasticsearch] failed to Count request")
		return 0, fmt.Errorf("count request failed: %w", err)
	}

	if res.Body != nil {
		defer func(Body io.ReadCloser) {
			closeBodyErr := Body.Close()
			if closeBodyErr != nil {
				log.WithFields(log.Fields{
					"error":  closeBodyErr.Error(),
					"index":  indexName,
					"source": "elasticsearch.CountDocuments",
				}).Error("[ElasticSearch] failed to close bulk response body")
			}
		}(res.Body)
	}

	if res.IsError() {
		bodyBytes, _ := io.ReadAll(res.Body)
		log.WithFields(log.Fields{
			"status": res.Status(),
			"index":  indexName,
			"body":   string(bodyBytes),
			"source": "elasticsearch.CountDocuments",
		}).Error("[Elasticsearch] count request failed for index")
		return 0, fmt.Errorf("count error: %s", string(bodyBytes))
	}

	var respBody struct {
		Count int64 `json:"count"`
	}
	if err := json.NewDecoder(res.Body).Decode(&respBody); err != nil {
		log.WithFields(log.Fields{
			"error":  err.Error(),
			"index":  indexName,
			"source": "elasticsearch.CountDocuments",
		}).Error("[Elasticsearch] failed to parse count response")
		return 0, fmt.Errorf("failed to parse count response: %w", err)
	}

	return respBody.Count, nil
}

func (c *Client) UpdateByQuery(index, routingKey string, body interface{}) error {
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(body); err != nil {
		log.WithFields(log.Fields{
			"error":  err.Error(),
			"index":  index,
			"source": "elasticsearch.UpdateByQuery",
		}).Error("[Elasticsearch] failed to encode update query")
		return fmt.Errorf("failed to encode update_by_query body: %w", err)
	}

	// Build request options
	reqOptions := []func(*esapi.UpdateByQueryRequest){
		c.es.UpdateByQuery.WithBody(&buf),
		c.es.UpdateByQuery.WithSlices("auto"),
		c.es.UpdateByQuery.WithScrollSize(env.GetEnvOrDefaultInt("ES_UPDATE_SCROLL_SIZE", 1000)),
		c.es.UpdateByQuery.WithConflicts("proceed"),
		c.es.UpdateByQuery.WithWaitForCompletion(false),
		c.es.UpdateByQuery.WithRequestsPerSecond(-1),
		c.es.UpdateByQuery.WithRefresh(false),
		c.es.UpdateByQuery.WithTimeout(10 * time.Minute),
		c.es.UpdateByQuery.WithContext(context.Background()),
	}

	if routingKey != "" {
		reqOptions = append(reqOptions, c.es.UpdateByQuery.WithRouting(routingKey))
	}

	// Perform the request
	res, err := c.es.UpdateByQuery(
		[]string{index},
		reqOptions...,
	)
	if err != nil {
		log.WithFields(log.Fields{
			"error":  err.Error(),
			"index":  index,
			"source": "elasticsearch.UpdateByQuery",
		}).Error("[Elasticsearch] failed to update index")
		return fmt.Errorf("update_by_query failed: %w", err)
	}

	defer func(Body io.ReadCloser) {
		closeBodyErr := Body.Close()
		if closeBodyErr != nil {
			log.WithFields(log.Fields{
				"error":  closeBodyErr.Error(),
				"index":  index,
				"source": "elasticsearch.UpdateByQuery",
			}).Error("[Elasticsearch] failed to close bulk response body")
		}
	}(res.Body)

	// Handle ES errors
	if res.IsError() {
		var esErr map[string]interface{}
		if errD := json.NewDecoder(res.Body).Decode(&esErr); errD != nil {
			log.WithFields(log.Fields{
				"error":  errD.Error(),
				"index":  index,
				"source": "elasticsearch.UpdateByQuery",
			}).Error("[Elasticsearch] failed to decode update index response")
			return fmt.Errorf("update_by_query error: %s", res.String())
		}

		log.WithFields(log.Fields{
			"status":      res.Status(),
			"index":       index,
			"source":      "elasticsearch.UpdateByQuery",
			"field_error": esErr,
		}).Error("[Elasticsearch] update index response failed")
		return fmt.Errorf("update_by_query error: %+v", esErr)
	}
	return nil
}
