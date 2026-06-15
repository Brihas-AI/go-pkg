package elasticsearch

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"

	log "github.com/sirupsen/logrus"
)

type IndexSettings struct {
	Shards                   int
	Replicas                 int
	RoutingAllocationPerNode int
}

func (c *Client) InitIndex(indexName string, mappingFilePath string, settings *IndexSettings) error {
	exists, err := c.IndexExists(indexName)
	if err != nil {
		log.WithFields(log.Fields{
			"err":       err.Error(),
			"indexName": indexName,
			"source":    "elasticsearch.InitIndex",
		}).Error("[Elasticsearch] failed to check if index exists")
		return err
	}
	if exists {
		return nil
	}

	mapping, err := loadMapping(mappingFilePath)
	if err != nil {
		log.WithFields(log.Fields{
			"err":       err.Error(),
			"indexName": indexName,
			"source":    "elasticsearch.InitIndex",
		}).Error("[Elasticsearch] failed to load mapping JSON")
		return err
	}

	if settings != nil {
		mergeSettings(mapping, settings)
	}

	if err = c.CreateIndex(indexName, mapping); err != nil {
		log.WithFields(log.Fields{
			"err":       err.Error(),
			"indexName": indexName,
			"source":    "elasticsearch.InitIndex",
		}).Error("[Elasticsearch] failed to create index")
		return fmt.Errorf("create index: %w", err)
	}

	fmt.Printf("Index '%s' created successfully.\n", indexName)
	return nil
}

func (c *Client) IndexExists(index string) (bool, error) {
	res, err := c.es.Indices.Exists([]string{index})

	if err != nil {
		log.WithFields(log.Fields{
			"error":     err.Error(),
			"indexName": index,
			"source":    "elasticsearch.IndexExists",
		}).Error("[Elasticsearch] failed to check if index exists")
		return false, fmt.Errorf("check index existence request: %w", err)
	}

	if res == nil {
		log.WithFields(log.Fields{
			"indexName": index,
			"source":    "elasticsearch.IndexExists",
		}).Error("[Elasticsearch] response is nil")
		return false, nil
	}

	if res.Body != nil {
		defer func(Body io.ReadCloser) {
			if err = Body.Close(); err != nil {
				log.WithFields(log.Fields{
					"err":    err.Error(),
					"index":  index,
					"source": "elasticsearch.IndexExists",
				}).Error("[Elasticsearch] failed to close response body")
			}
		}(res.Body)
	}

	switch res.StatusCode {
	case 200:
		return true, nil
	case 404:
		return false, nil
	default:
		body, _ := io.ReadAll(res.Body)
		return false, fmt.Errorf("unexpected status from index exists: %s", body)
	}
}

func (c *Client) CreateIndex(index string, mapping map[string]interface{}) error {
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(mapping); err != nil {
		log.WithFields(log.Fields{
			"err":    err.Error(),
			"index":  index,
			"source": "elasticsearch.CreateIndex",
		}).Error("[Elasticsearch] failed to encode mapping JSON")
		return fmt.Errorf("encode mapping: %w", err)
	}

	res, err := c.es.Indices.Create(index, c.es.Indices.Create.WithBody(&buf))
	if err != nil {
		log.WithFields(log.Fields{
			"err":    err.Error(),
			"index":  index,
			"source": "elasticsearch.CreateIndex",
		}).Error("[Elasticsearch] failed to send index creation request")
		return fmt.Errorf("create index request: %w", err)
	}
	defer func(Body io.ReadCloser) {
		if err = Body.Close(); err != nil {
			log.WithFields(log.Fields{
				"err":    err.Error(),
				"index":  index,
				"source": "elasticsearch.CreateIndex",
			}).Error("[Elasticsearch] failed to close response body")
		}
	}(res.Body)

	if res.IsError() {
		body, _ := io.ReadAll(res.Body)
		return fmt.Errorf("elasticsearch returned error response: %s", body)
	}

	return nil
}

func loadMapping(path string) (map[string]interface{}, error) {
	absPath, err := filepath.Abs(path)
	if err != nil {
		log.WithFields(log.Fields{
			"err":    err.Error(),
			"path":   path,
			"source": "elasticsearch.LoadMapping",
		}).Error("[Elasticsearch] failed to resolve absolute path")
		return nil, fmt.Errorf("resolve path: %w", err)
	}

	data, err := os.ReadFile(absPath)
	if err != nil {
		log.WithFields(log.Fields{
			"err":    err.Error(),
			"path":   absPath,
			"source": "elasticsearch.LoadMapping",
		}).Error("[Elasticsearch] failed to read mapping file")
		return nil, fmt.Errorf("read mapping file: %w", err)
	}

	var mapping map[string]interface{}
	if err = json.Unmarshal(data, &mapping); err != nil {
		log.WithFields(log.Fields{
			"err":    err.Error(),
			"path":   absPath,
			"source": "elasticsearch.LoadMapping",
		}).Error("[Elasticsearch] failed to parse mapping JSON")
		return nil, fmt.Errorf("unmarshal mapping JSON: %w", err)
	}

	return mapping, nil
}

func mergeSettings(mapping map[string]interface{}, cfg *IndexSettings) {
	if mapping["settings"] == nil {
		mapping["settings"] = make(map[string]interface{})
	}
	settings := mapping["settings"].(map[string]interface{})
	settings["number_of_shards"] = cfg.Shards
	settings["number_of_replicas"] = cfg.Replicas
	settings["routing.allocation.total_shards_per_node"] = cfg.RoutingAllocationPerNode
}
