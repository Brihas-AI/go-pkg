package rabbitmq

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	log "github.com/sirupsen/logrus"
)

var (
	producerOnce   sync.Once
	producerClient *Producer
)

type Producer struct {
	conn   *amqp.Connection
	ch     *amqp.Channel
	connMu sync.RWMutex

	url      string
	exchange string

	isConnected       atomic.Bool
	isReconnecting    atomic.Bool
	isShuttingDown    atomic.Bool
	isBlocked         atomic.Bool
	reconnectAttempts atomic.Int32

	queues     map[string]QueueConfig
	topologyMu sync.RWMutex

	monitorDone chan struct{}
	monitorOnce sync.Once

	reconnectMu sync.Mutex
}

type QueueConfig struct {
	Name       string
	Durable    bool
	AutoDelete bool
	Exclusive  bool
	NoWait     bool
	Args       amqp.Table
	RoutingKey string
}

const (
	initialRetryDelay    = 1 * time.Second
	maxRetryDelay        = 60 * time.Second
	maxReconnectAttempts = 10
	maxPublishRetries    = 3
	heartbeatInterval    = 30 * time.Second
	connectionTimeout    = 30 * time.Second
)

func InitProducer() {
	producerOnce.Do(func() {
		client, err := NewProducer()
		if err != nil {
			log.WithError(err).Fatal("[RabbitMQ] failed to initialize producer")
		}
		producerClient = client
		log.Info("[RabbitMQ] producer initialized successfully")
	})
}

func GetProducerClient() *Producer {
	return producerClient
}

func NewProducer() (*Producer, error) {
	host := os.Getenv("RABBITMQ_HOST")
	port := os.Getenv("RABBITMQ_PORT")
	user := os.Getenv("RABBITMQ_USERNAME")
	pass := os.Getenv("RABBITMQ_PASSWORD")

	if host == "" || port == "" || user == "" || pass == "" {
		return nil, fmt.Errorf("RabbitMQ env vars missing")
	}

	url := fmt.Sprintf("amqp://%s:%s@%s:%s/", user, pass, host, port)
	exchange := "events-exchange"

	p := &Producer{
		url:         url,
		exchange:    exchange,
		queues:      make(map[string]QueueConfig),
		monitorDone: make(chan struct{}),
	}

	if err := p.Connect(); err != nil {
		return nil, err
	}

	p.monitorOnce.Do(func() {
		go p.persistentMonitor()
	})

	return p, nil
}

func (p *Producer) Connect() error {
	p.connMu.Lock()
	defer p.connMu.Unlock()

	// Check if already connected
	if p.isConnected.Load() {
		return fmt.Errorf("already connected")
	}
	if p.isShuttingDown.Load() {
		return fmt.Errorf("shutting down")
	}

	p.isReconnecting.Store(true)
	defer p.isReconnecting.Store(false)

	p.closeConnectionsUnsafe()

	cfg := amqp.Config{
		Heartbeat:  heartbeatInterval,
		Locale:     "en_US",
		Dial:       amqp.DefaultDial(connectionTimeout),
		Properties: amqp.NewConnectionProperties(),
	}
	cfg.Properties.SetClientConnectionName("generic-rabbitmq-producer")

	log.Info("[RabbitMQ] attempting to connect...")

	conn, err := amqp.DialConfig(p.url, cfg)
	if err != nil {
		return fmt.Errorf("connection failed: %w", err)
	}

	ch, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		return fmt.Errorf("failed to open channel: %w", err)
	}

	if err := ch.Qos(10, 0, false); err != nil {
		_ = ch.Close()
		_ = conn.Close()
		return fmt.Errorf("failed to set QoS: %w", err)
	}

	if err := ch.ExchangeDeclare(
		p.exchange,
		"direct",
		true,
		false,
		false,
		false,
		nil,
	); err != nil {
		_ = ch.Close()
		_ = conn.Close()
		return fmt.Errorf("failed to declare exchange: %w", err)
	}

	p.conn = conn
	p.ch = ch

	p.isConnected.Store(true)
	p.isBlocked.Store(false)
	p.reconnectAttempts.Store(0)

	log.Info("[RabbitMQ] producer connected successfully")

	if err := p.setupTopology(); err != nil {
		log.WithError(err).Error("[RabbitMQ] failed to restore topology")
		return err
	}

	return nil
}

func (p *Producer) setupTopology() error {
	p.topologyMu.RLock()
	defer p.topologyMu.RUnlock()

	for _, qCfg := range p.queues {
		if _, err := p.ch.QueueDeclare(
			qCfg.Name,
			qCfg.Durable,
			qCfg.AutoDelete,
			qCfg.Exclusive,
			qCfg.NoWait,
			qCfg.Args,
		); err != nil {
			return fmt.Errorf("failed to declare queue %s: %w", qCfg.Name, err)
		}

		if err := p.ch.QueueBind(
			qCfg.Name,
			qCfg.RoutingKey,
			p.exchange,
			false,
			nil,
		); err != nil {
			return fmt.Errorf("failed to bind queue %s: %w", qCfg.Name, err)
		}
	}

	if len(p.queues) > 0 {
		log.Infof("[RabbitMQ] restored %d queues", len(p.queues))
	}
	return nil
}

func (p *Producer) Publish(topic string, _ string, message interface{}) error {
	return p.PublishWithContext(context.Background(), topic, message)
}

func (p *Producer) PublishWithContext(ctx context.Context, topic string, message interface{}) error {
	if !p.isConnected.Load() {
		return fmt.Errorf("not connected")
	}

	if p.isBlocked.Load() {
		return fmt.Errorf("connection blocked")
	}

	topic = strings.TrimSpace(topic)
	if topic == "" {
		return fmt.Errorf("empty topic name")
	}

	var body []byte
	switch v := message.(type) {
	case []byte:
		body = v
	case string:
		body = []byte(v)
	default:
		b, err := json.Marshal(v)
		if err != nil {
			return fmt.Errorf("failed to marshal message: %w", err)
		}
		body = b
	}

	if err := p.ensureQueue(topic); err != nil {
		return err
	}

	p.connMu.RLock()
	ch := p.ch
	p.connMu.RUnlock()

	if ch == nil || ch.IsClosed() {
		return fmt.Errorf("channel closed")
	}

	var lastErr error
	for attempt := 1; attempt <= maxPublishRetries; attempt++ {
		err := ch.PublishWithContext(
			ctx,
			p.exchange,
			topic,
			false,
			false,
			amqp.Publishing{
				ContentType:  "application/json",
				DeliveryMode: amqp.Persistent,
				Body:         body,
				Timestamp:    time.Now(),
			},
		)

		if err == nil {
			return nil
		}

		lastErr = err
		log.Warnf("[RabbitMQ] publish attempt %d/%d failed: %v", attempt, maxPublishRetries, err)

		if attempt < maxPublishRetries {
			time.Sleep(time.Second)
		}
	}

	return fmt.Errorf("failed to publish after %d attempts: %w", maxPublishRetries, lastErr)
}

// ensureQueue declares queue and stores topology
func (p *Producer) ensureQueue(topic string) error {
	p.topologyMu.Lock()
	defer p.topologyMu.Unlock()

	// Check if already declared
	if _, exists := p.queues[topic]; exists {
		return nil
	}

	p.connMu.RLock()
	ch := p.ch
	p.connMu.RUnlock()

	if ch == nil || ch.IsClosed() {
		return fmt.Errorf("channel not available")
	}

	qCfg := QueueConfig{
		Name:       topic,
		Durable:    true,
		AutoDelete: false,
		Exclusive:  false,
		NoWait:     false,
		Args:       amqp.Table{"x-queue-mode": "lazy"},
		RoutingKey: topic,
	}

	if _, err := ch.QueueDeclare(
		qCfg.Name,
		qCfg.Durable,
		qCfg.AutoDelete,
		qCfg.Exclusive,
		qCfg.NoWait,
		qCfg.Args,
	); err != nil {
		return fmt.Errorf("failed to declare queue: %w", err)
	}

	if err := ch.QueueBind(topic, topic, p.exchange, false, nil); err != nil {
		return fmt.Errorf("failed to bind queue: %w", err)
	}

	p.queues[topic] = qCfg

	return nil
}

func (p *Producer) persistentMonitor() {
	for {
		select {
		case <-p.monitorDone:
			log.Info("[RabbitMQ] monitor goroutine stopped")
			return
		default:
		}

		p.connMu.RLock()
		conn := p.conn
		ch := p.ch
		p.connMu.RUnlock()

		if conn == nil || ch == nil {
			time.Sleep(5 * time.Second)
			continue
		}

		connClose := conn.NotifyClose(make(chan *amqp.Error, 1))
		chanClose := ch.NotifyClose(make(chan *amqp.Error, 1))
		blocked := conn.NotifyBlocked(make(chan amqp.Blocking, 1))

		p.monitorCurrentConnection(connClose, chanClose, blocked)

		if !p.isShuttingDown.Load() {
			p.handleReconnect()
		}
	}
}

func (p *Producer) monitorCurrentConnection(
	connClose <-chan *amqp.Error,
	chanClose <-chan *amqp.Error,
	blocked <-chan amqp.Blocking,
) {
	for {
		select {
		case <-p.monitorDone:
			return

		case blocking := <-blocked:
			p.isBlocked.Store(blocking.Active)
			if blocking.Active {
				log.Warnf("[RabbitMQ] connection blocked: %s", blocking.Reason)
			} else {
				log.Info("[RabbitMQ] connection unblocked")
			}

		case err := <-chanClose:
			if err != nil {
				log.Errorf("[RabbitMQ] channel closed: %v", err)
				p.isConnected.Store(false)
				return
			}

		case err := <-connClose:
			if err != nil {
				log.Errorf("[RabbitMQ] connection closed: %v", err)
				p.isConnected.Store(false)
				return
			}
		}
	}
}

func (p *Producer) handleReconnect() {
	if !p.reconnectMu.TryLock() {
		log.Debug("[RabbitMQ] reconnection already in progress")
		return
	}
	defer p.reconnectMu.Unlock()

	if p.isShuttingDown.Load() {
		return
	}

	for {
		attempts := p.reconnectAttempts.Load()

		if p.isShuttingDown.Load() {
			return
		}

		if attempts >= maxReconnectAttempts {
			log.Error("[RabbitMQ] max reconnection attempts exceeded")
			return
		}

		backoff := initialRetryDelay * time.Duration(1<<uint(attempts))
		if backoff > maxRetryDelay {
			backoff = maxRetryDelay
		}

		currentAttempt := p.reconnectAttempts.Add(1)

		log.Infof("[RabbitMQ] reconnection attempt %d/%d (waiting %v)",
			currentAttempt, maxReconnectAttempts, backoff)

		time.Sleep(backoff)

		if err := p.Connect(); err == nil {
			log.Info("[RabbitMQ] successfully reconnected")
			return
		} else {
			log.WithError(err).Warn("[RabbitMQ] reconnection failed")
		}
	}
}

func (p *Producer) IsConnected() bool {
	return p.isConnected.Load()
}

func (p *Producer) IsBlocked() bool {
	return p.isBlocked.Load()
}

func (p *Producer) Close() {
	if !p.isShuttingDown.CompareAndSwap(false, true) {
		// Already shutting down
		return
	}

	log.Info("[RabbitMQ] shutting down producer...")

	close(p.monitorDone)

	time.Sleep(100 * time.Millisecond)

	p.connMu.Lock()
	p.closeConnectionsUnsafe()
	p.connMu.Unlock()

	log.Info("[RabbitMQ] producer closed gracefully")
}

func (p *Producer) closeConnectionsUnsafe() {
	if p.ch != nil && !p.ch.IsClosed() {
		_ = p.ch.Close()
		p.ch = nil
	}
	if p.conn != nil && !p.conn.IsClosed() {
		_ = p.conn.Close()
		p.conn = nil
	}
}
