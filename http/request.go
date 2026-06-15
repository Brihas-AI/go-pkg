package Http

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/Brihas-AI/go-pkg/env"
	"github.com/Brihas-AI/go-pkg/utils"
	log "github.com/sirupsen/logrus"
	"moul.io/http2curl"
)

type Request[Req, Res any] struct {
	URL         string
	Method      string
	APIName     string
	Timeout     Timeout
	Headers     http.Header
	Params      url.Values
	Payload     *Req
	payloadRaw  []byte
	RetryPolicy *RetryPolicy
	Context     context.Context
}

type RetryPolicy struct {
	MaxAttempts int
	Timeout     Timeout
	Backoff     time.Duration
}

func (r *Request[Req, Res]) init() (*Request[Req, Res], error) {
	if r == nil {
		return nil, errors.New("request is nil")
	}
	if r.URL = strings.TrimSpace(r.URL); r.URL == "" {
		return nil, errors.New("request URL is empty")
	}
	if r.APIName = strings.TrimSpace(r.APIName); r.APIName == "" {
		return nil, errors.New("API name is empty")
	}
	if r.Method = strings.ToUpper(strings.TrimSpace(r.Method)); r.Method == "" {
		r.Method = http.MethodGet
	}
	r.APIName = fmt.Sprintf("%s->%s", r.Method, r.APIName)
	return r, nil
}

func (r *Request[Req, Res]) Execute() (*Res, ErrRequest) {
	resp, err := r.do()
	if err != nil {
		return nil, err
	}
	if resp == nil || resp.Body == nil {
		return nil, NewErrEmptyResponse()
	}

	defer func(body io.ReadCloser) {
		if body != nil {
			if closeErr := body.Close(); closeErr != nil {
				r.logError(closeErr, "ERR_CLOSE_RESPONSE_BODY")
			}
		}
	}(resp.Body)

	var responseData Res
	if decodeErr := json.NewDecoder(resp.Body).Decode(&responseData); decodeErr != nil {
		return nil, NewErrJSONDecode()
	}
	return &responseData, nil
}

func (r *Request[Req, Res]) do() (*http.Response, ErrRequest) {
	if _, err := r.init(); err != nil {
		r.logError(err, "ERR_REQUEST_INIT")
		return nil, NewErrRequestInit()
	}

	if r.Payload != nil {
		var err error
		r.payloadRaw, err = json.Marshal(r.Payload)
		if err != nil {
			r.logError(err, "ERR_JSON_ENCODE")
			return nil, NewErrJSONEncode()
		}
		if len(r.payloadRaw) > 500 {
			r.Payload = nil // avoid verbose log
		}
	}

	parsedURL, err := url.Parse(r.URL)
	if err != nil {
		r.logError(err, "ERR_URL_PARSE")
		return nil, NewErrURLParse()
	}

	if len(r.Params) > 0 {
		parsedURL.RawQuery = r.Params.Encode()
	}

	ctx := r.Context
	if ctx == nil {
		ctx = context.Background()
	}

	httpReq, err := http.NewRequestWithContext(ctx, r.Method, parsedURL.String(), bytes.NewBuffer(r.payloadRaw))
	if err != nil {
		r.logError(err, "ERR_CREATE_REQUEST")
		return nil, NewErrRequestCreate()
	}
	httpReq.Header = r.Headers

	if env.GetEnvOrDefaultBool("LOG_HTTP_CURL", false) {
		apiName := strings.ToLower(strings.TrimSpace(r.APIName))
		debugApiName := strings.ToLower(strings.TrimSpace(os.Getenv("DEBUG_CURL_API_NAME")))
		if curlCmd, _ := http2curl.GetCurlCommand(httpReq); curlCmd != nil && apiName == debugApiName {
			fmt.Println("CURL:", curlCmd.String())
		}
	}

	client := getClientWithTimeout(r.Timeout)
	resp, err := client.Do(httpReq)
	if err != nil && r.RetryPolicy != nil {
		resp, err = r.retry(client, parsedURL)
	}
	if err != nil {
		var netErr net.Error
		if errors.As(err, &netErr) && netErr.Timeout() {
			r.logError(err, "ERR_TIMEOUT")
			return nil, NewErrTimeout()
		}
		if errors.Is(err, context.DeadlineExceeded) {
			r.logError(err, "ERR_TIMEOUT")
			return nil, NewErrTimeout()
		}
		if errors.Is(err, context.Canceled) {
			r.logError(err, "ERR_CANCELED")
			return nil, NewErrCanceled()
		}
		r.logError(err, "ERR_REQUEST_FAILED")
		return nil, NewErrRequestFailed(err)
	}

	if resp.StatusCode != http.StatusOK {
		r.logError(err, "ERR_NON_200")
		return resp, NewErrNon200(resp.StatusCode)
	}

	return resp, nil
}

func (r *Request[Req, Res]) retry(client *http.Client, parsedURL *url.URL) (*http.Response, error) {
	for attempt := 0; attempt < r.RetryPolicy.MaxAttempts; attempt++ {
		ctx := r.Context
		if ctx == nil {
			ctx = context.Background()
		}
		req, err := http.NewRequestWithContext(ctx, r.Method, parsedURL.String(), bytes.NewBuffer(r.payloadRaw))
		if err != nil {
			r.logError(err, "ERR_RETRY_CREATE_REQUEST")
			continue
		}
		req.Header = r.Headers

		if r.RetryPolicy.Timeout > 0 {
			client = getClientWithTimeout(r.RetryPolicy.Timeout)
		}
		if r.RetryPolicy.Backoff > 0 {
			delay := time.Duration(utils.GetNthFibonacciNumber(attempt)) * r.RetryPolicy.Backoff
			time.Sleep(delay)
		}

		resp, err := client.Do(req)
		if err == nil {
			return resp, nil
		}
	}
	return nil, fmt.Errorf("retry attempts exhausted")
}

func (r *Request[Req, Res]) logError(err error, code string) {
	log.WithFields(log.Fields{
		"url":     r.URL,
		"method":  r.Method,
		"headers": r.Headers,
		"params":  r.Params,
		"payload": r.Payload,
		"error":   err,
		"code":    code,
	}).Error("Request execution failed")
}
