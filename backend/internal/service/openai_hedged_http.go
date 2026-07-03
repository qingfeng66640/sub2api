package service

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"

	"github.com/gin-gonic/gin"
)

func openAIAPIKeyFromGin(c *gin.Context) *APIKey {
	if c == nil {
		return nil
	}
	if v, ok := c.Get("api_key"); ok {
		if apiKey, ok := v.(*APIKey); ok {
			return apiKey
		}
	}
	return nil
}

func clonePreparedRequestWithContext(req *http.Request, ctx context.Context) (*http.Request, error) {
	if req == nil {
		return nil, fmt.Errorf("nil upstream request")
	}
	cloned := req.Clone(ctx)
	if req.GetBody != nil {
		body, err := req.GetBody()
		if err != nil {
			return nil, err
		}
		cloned.Body = body
		return cloned, nil
	}
	if req.Body == nil || req.Body == http.NoBody {
		cloned.Body = http.NoBody
		return cloned, nil
	}
	data, err := io.ReadAll(req.Body)
	if err != nil {
		return nil, err
	}
	_ = req.Body.Close()
	req.Body = io.NopCloser(bytes.NewReader(data))
	cloned.Body = io.NopCloser(bytes.NewReader(data))
	return cloned, nil
}

func primeResponseFirstByte(resp *http.Response) (*http.Response, error) {
	if resp == nil || resp.Body == nil || resp.Body == http.NoBody {
		return resp, nil
	}
	buf := make([]byte, 1)
	n, err := resp.Body.Read(buf)
	if n > 0 {
		resp.Body = struct {
			io.Reader
			io.Closer
		}{
			Reader: io.MultiReader(bytes.NewReader(buf[:n]), resp.Body),
			Closer: resp.Body,
		}
		return resp, nil
	}
	if err == io.EOF {
		return resp, nil
	}
	if err != nil {
		_ = resp.Body.Close()
		return nil, err
	}
	return resp, nil
}

func (s *OpenAIGatewayService) doOpenAIHedgedPreparedHTTP(
	ctx context.Context,
	c *gin.Context,
	upstreamReq *http.Request,
	proxyURL string,
	account *Account,
) (*http.Response, *HedgedMetadata, error) {
	if account == nil {
		return nil, nil, fmt.Errorf("nil account")
	}
	apiKey := openAIAPIKeyFromGin(c)
	policy := HedgePolicyFromAPIKey(apiKey)
	if !policy.Enabled || policy.MaxParallelCount <= 1 {
		resp, err := s.httpUpstream.Do(upstreamReq, proxyURL, account.ID, account.Concurrency)
		return resp, nil, err
	}

	policy = policy.Normalize()
	total := policy.InitialParallelCount + policy.DelayedParallelCount
	if total > policy.MaxParallelCount {
		total = policy.MaxParallelCount
	}
	if total < 1 {
		total = 1
	}

	attempts := make([]HedgedHTTPAttempt, total)
	for i := range attempts {
		idx := i
		attempts[i] = HedgedHTTPAttempt{
			Index: idx,
			Start: func(attemptCtx context.Context) (*http.Response, error) {
				req, err := clonePreparedRequestWithContext(upstreamReq, attemptCtx)
				if err != nil {
					return nil, err
				}
				resp, err := s.httpUpstream.Do(req, proxyURL, account.ID, account.Concurrency)
				if err != nil {
					return nil, err
				}
				return primeResponseFirstByte(resp)
			},
		}
	}

	result, err := RaceHedgedHTTP(ctx, policy, attempts)
	if err != nil {
		return nil, nil, err
	}
	return result.Response, &result.Meta, nil
}
