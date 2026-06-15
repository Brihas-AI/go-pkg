package Http

import (
	"net"
	"net/http"
	"os"
	"sync"
	"time"

	"go.elastic.co/apm/module/apmhttp/v2"
)

type APIRequest[T any] interface {
	Validate() error
	GetResponse() (T, error)
}

var (
	clientCache sync.Map
)

func createClient(timeout time.Duration) *http.Client {
	maxIdleConnections := 150
	defaultTransport := http.Transport{
		DialContext: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          maxIdleConnections,
		MaxIdleConnsPerHost:   maxIdleConnections,
		MaxConnsPerHost:       maxIdleConnections,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}

	var rt http.RoundTripper = &defaultTransport
	if os.Getenv("ELASTIC_APM_ACTIVE") != "false" {
		rt = apmhttp.WrapRoundTripper(rt)
	}

	return &http.Client{
		Timeout:   timeout,
		Transport: rt,
	}
}

func getClientWithTimeout(timeout Timeout) *http.Client {
	d := timeout.GetDuration()

	if cached, ok := clientCache.Load(d); ok {
		if client, ok := cached.(*http.Client); ok {
			return client
		}
	}

	newClient := createClient(d)
	clientCache.Store(d, newClient)
	return newClient
}
