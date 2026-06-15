package kafka

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	log "github.com/sirupsen/logrus"

	"golang.org/x/oauth2/google"
)

type TokenProvider struct {
	GCPCredentials string
	clientEmail    string
}

func NewTokenProvider(gcpCredentials string) *TokenProvider {
	return &TokenProvider{GCPCredentials: gcpCredentials}
}

func (t *TokenProvider) GenerateToken() (string, error) {
	ctx := context.Background()

	// GOOGLE_APPLICATION_CREDENTIALS is always a file path, not inline JSON.
	// Read file content if the value looks like a path.
	creds := t.GCPCredentials
	if strings.HasPrefix(creds, "/") || strings.HasPrefix(creds, ".") {
		data, err := os.ReadFile(creds)
		if err != nil {
			log.WithFields(log.Fields{
				"source": "kafka.GenerateToken",
				"path":   creds,
				"error":  err,
			}).Error("[Kafka] failed to read credentials file.")
			return "", fmt.Errorf("[Kafka] failed to read credentials file: %w", err)
		}
		creds = string(data)
	}
	jsonBytes := []byte(creds)

	credential, err := google.CredentialsFromJSON(ctx, jsonBytes, "https://www.googleapis.com/auth/cloud-platform")
	if err != nil {
		log.WithFields(log.Fields{
			"source": "kafka.GenerateToken",
			"error":  err,
		}).Error("[Kafka] failed to parse credentials.")
		return "", fmt.Errorf("[Kafka] failed to parse credentials: %w", err)
	}

	token, err := credential.TokenSource.Token()
	if err != nil {
		log.WithFields(log.Fields{
			"source": "kafka.GenerateToken",
			"error":  err,
		}).Error("[Kafka] failed to get token.")
		return "", fmt.Errorf("[Kafka] failed to get token: %w", err)
	}

	var credsMap map[string]interface{}
	if err = json.Unmarshal(jsonBytes, &credsMap); err != nil {
		log.WithFields(log.Fields{
			"source": "kafka.GenerateToken",
			"error":  err,
		}).Error("[Kafka] failed to unmarshal credentials.")
		return "", fmt.Errorf("[Kafka] unmarshal credentials: %w", err)
	}
	clientEmail, ok := credsMap["client_email"].(string)
	if !ok {
		log.WithFields(log.Fields{
			"source": "kafka.GenerateToken",
			"error":  err,
		}).Error("[Kafka] failed to get client_email from credentials.")
		return "", fmt.Errorf("[Kafka] client_email is not found in credentials")
	}
	t.clientEmail = clientEmail

	now := time.Now().UTC()
	expiration := now.Add(time.Hour)

	payload := map[string]interface{}{
		"exp":   expiration.Unix(),
		"iat":   now.Unix(),
		"scope": "kafka",
		"sub":   t.clientEmail,
	}

	header := base64.RawURLEncoding.EncodeToString([]byte(`{"typ":"JWT","alg":"GOOG_OAUTH2_TOKEN"}`))
	body, _ := json.Marshal(payload)
	bodyStr := base64.RawURLEncoding.EncodeToString(body)
	sig := base64.RawURLEncoding.EncodeToString([]byte(token.AccessToken))

	return fmt.Sprintf("%s.%s.%s", header, bodyStr, sig), nil
}

func (p *Producer) tokenRefresher() {
	for {
		token, err := p.tokenProvider.GenerateToken()
		if err != nil {
			time.Sleep(5 * time.Minute)
			continue
		}
		err = p.client.SetOAuthBearerToken(kafka.OAuthBearerToken{
			TokenValue: token,
			Expiration: time.Now().Add(time.Hour),
		})
		if err != nil {
			log.WithFields(log.Fields{
				"token":  token,
				"source": "kafka.tokenRefresher",
				"error":  err,
			}).Error("[Kafka] failed to set token. This may be due to an expired token.")
		}
		time.Sleep(55*time.Minute + time.Duration(rand.Intn(60))*time.Second)
	}
}

func (c *Consumer) tokenRefresher() {
	for {
		token, err := c.tokenProvider.GenerateToken()
		if err != nil {
			time.Sleep(5 * time.Minute)
			continue
		}
		err = c.client.SetOAuthBearerToken(kafka.OAuthBearerToken{
			TokenValue: token,
			Expiration: time.Now().Add(time.Hour),
		})
		if err != nil {
			log.WithFields(log.Fields{
				"token":  token,
				"source": "kafka.tokenRefresher",
				"error":  err,
			}).Error("[Kafka] failed to set token. This may be due to an expired token.")
		}
		time.Sleep(55*time.Minute + time.Duration(rand.Intn(60))*time.Second)
	}
}
