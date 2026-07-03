package service

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"sync"
	"time"
)

const (
	HedgeRouteStrategySameAccount = "same_account"

	HedgeAttemptStatusWinner   = "winner"
	HedgeAttemptStatusCanceled = "canceled"
	HedgeAttemptStatusError    = "error"
)

type HedgePolicy struct {
	Enabled               bool
	InitialParallelCount  int
	Delay                 time.Duration
	DelayedParallelCount  int
	MaxParallelCount      int
	RouteStrategy         string
}

type HedgedAttemptLog struct {
	Index        int    `json:"index"`
	Status       string `json:"status"`
	DurationMs   int64  `json:"duration_ms,omitempty"`
	FirstTokenMs int64  `json:"first_token_ms,omitempty"`
	Error        string `json:"error,omitempty"`
}

type HedgedMetadata struct {
	Enabled       bool
	AttemptCount  int
	WinnerIndex   *int
	CanceledCount int
	ErrorCount    int
	Attempts      []HedgedAttemptLog
}

type HedgedHTTPAttempt struct {
	Index int
	Start func(ctx context.Context) (*http.Response, error)
}

type HedgedHTTPResult struct {
	Index    int
	Response *http.Response
	Meta     HedgedMetadata
}

func DefaultHedgePolicy() HedgePolicy {
	return HedgePolicy{
		Enabled:               false,
		InitialParallelCount:  1,
		Delay:                 10 * time.Second,
		DelayedParallelCount:  1,
		MaxParallelCount:      2,
		RouteStrategy:         HedgeRouteStrategySameAccount,
	}
}

func HedgePolicyFromAPIKey(apiKey *APIKey) HedgePolicy {
	policy := DefaultHedgePolicy()
	if apiKey == nil || !apiKey.AccelerationEnabled || !apiKey.HedgeEnabled {
		return policy
	}
	policy.Enabled = true
	policy.InitialParallelCount = apiKey.HedgeInitialParallelCount
	policy.Delay = time.Duration(apiKey.HedgeDelaySeconds * float64(time.Second))
	policy.DelayedParallelCount = apiKey.HedgeDelayedParallelCount
	policy.MaxParallelCount = apiKey.HedgeMaxParallelCount
	policy.RouteStrategy = apiKey.HedgeRouteStrategy
	return policy.Normalize()
}

func (p HedgePolicy) Normalize() HedgePolicy {
	if p.InitialParallelCount < 1 {
		p.InitialParallelCount = 1
	}
	if p.DelayedParallelCount < 0 {
		p.DelayedParallelCount = 0
	}
	if p.MaxParallelCount < 1 {
		p.MaxParallelCount = 1
	}
	if p.InitialParallelCount > p.MaxParallelCount {
		p.InitialParallelCount = p.MaxParallelCount
	}
	if p.Delay < 0 {
		p.Delay = 0
	}
	if strings.TrimSpace(p.RouteStrategy) == "" {
		p.RouteStrategy = HedgeRouteStrategySameAccount
	}
	return p
}

func ValidateHedgeConfig(initial int, delaySeconds float64, delayed int, max int, routeStrategy string) error {
	if initial < 1 || initial > 10 {
		return errors.New("hedge_initial_parallel_count must be between 1 and 10")
	}
	if delaySeconds < 0 || delaySeconds > 120 {
		return errors.New("hedge_delay_seconds must be between 0 and 120")
	}
	if delayed < 0 || delayed > 10 {
		return errors.New("hedge_delayed_parallel_count must be between 0 and 10")
	}
	if max < 1 || max > 10 {
		return errors.New("hedge_max_parallel_count must be between 1 and 10")
	}
	if initial > max {
		return errors.New("hedge_initial_parallel_count cannot exceed hedge_max_parallel_count")
	}
	if strings.TrimSpace(routeStrategy) != HedgeRouteStrategySameAccount {
		return errors.New("hedge_route_strategy must be same_account")
	}
	return nil
}

func RaceHedgedHTTP(ctx context.Context, policy HedgePolicy, attempts []HedgedHTTPAttempt) (*HedgedHTTPResult, error) {
	policy = policy.Normalize()
	if len(attempts) == 0 {
		return nil, errors.New("no hedge attempts")
	}
	if !policy.Enabled || policy.MaxParallelCount <= 1 {
		started := time.Now()
		resp, err := attempts[0].Start(ctx)
		meta := HedgedMetadata{Enabled: false, AttemptCount: 1, Attempts: []HedgedAttemptLog{{Index: attempts[0].Index, DurationMs: time.Since(started).Milliseconds()}}}
		if err != nil {
			meta.ErrorCount = 1
			meta.Attempts[0].Status = HedgeAttemptStatusError
			meta.Attempts[0].Error = err.Error()
			return nil, err
		}
		winner := attempts[0].Index
		meta.WinnerIndex = &winner
		meta.Attempts[0].Status = HedgeAttemptStatusWinner
		return &HedgedHTTPResult{Index: attempts[0].Index, Response: resp, Meta: meta}, nil
	}

	maxAttempts := policy.InitialParallelCount + policy.DelayedParallelCount
	if maxAttempts > policy.MaxParallelCount {
		maxAttempts = policy.MaxParallelCount
	}
	if maxAttempts > len(attempts) {
		maxAttempts = len(attempts)
	}
	if maxAttempts < 1 {
		maxAttempts = 1
	}

	meta := HedgedMetadata{Enabled: true, Attempts: make([]HedgedAttemptLog, maxAttempts)}
	for i := range meta.Attempts {
		meta.Attempts[i].Index = attempts[i].Index
	}

	type attemptResult struct {
		pos      int
		index    int
		resp     *http.Response
		err      error
		duration time.Duration
	}

	parent, cancelAll := context.WithCancel(ctx)
	defer cancelAll()

	resultCh := make(chan attemptResult, maxAttempts)
	cancels := make([]context.CancelFunc, maxAttempts)
	var startMu sync.Mutex
	started := 0

	finalizeMeta := func() HedgedMetadata {
		actualStarted := started
		if actualStarted < 0 {
			actualStarted = 0
		}
		if actualStarted > len(meta.Attempts) {
			actualStarted = len(meta.Attempts)
		}
		meta.AttemptCount = actualStarted
		meta.Attempts = meta.Attempts[:actualStarted]
		return meta
	}

	startAttempt := func(pos int) {
		startMu.Lock()
		if pos >= maxAttempts || cancels[pos] != nil {
			startMu.Unlock()
			return
		}
		attemptCtx, cancel := context.WithCancel(parent)
		cancels[pos] = cancel
		started++
		attempt := attempts[pos]
		startMu.Unlock()

		go func() {
			begin := time.Now()
			resp, err := attempt.Start(attemptCtx)
			resultCh <- attemptResult{pos: pos, index: attempt.Index, resp: resp, err: err, duration: time.Since(begin)}
		}()
	}

	initial := policy.InitialParallelCount
	if initial > maxAttempts {
		initial = maxAttempts
	}
	for i := 0; i < initial; i++ {
		startAttempt(i)
	}

	delayedStarted := false
	var timer <-chan time.Time
	if policy.DelayedParallelCount > 0 && initial < maxAttempts {
		if policy.Delay == 0 {
			for i := initial; i < maxAttempts; i++ {
				startAttempt(i)
			}
			delayedStarted = true
		} else {
			t := time.NewTimer(policy.Delay)
			defer t.Stop()
			timer = t.C
		}
	}

	completed := 0
	for completed < maxAttempts {
		select {
		case <-ctx.Done():
			cancelAll()
			return nil, ctx.Err()
		case <-timer:
			if !delayedStarted {
				for i := initial; i < maxAttempts; i++ {
					startAttempt(i)
				}
				delayedStarted = true
			}
			timer = nil
		case res := <-resultCh:
			completed++
			meta.Attempts[res.pos].DurationMs = res.duration.Milliseconds()
			if res.err == nil && res.resp != nil {
				winner := res.index
				meta.WinnerIndex = &winner
				meta.Attempts[res.pos].Status = HedgeAttemptStatusWinner
				for i, cancel := range cancels {
					if i == res.pos || cancel == nil {
						continue
					}
					cancel()
					if meta.Attempts[i].Status == "" {
						meta.Attempts[i].Status = HedgeAttemptStatusCanceled
						meta.CanceledCount++
					}
				}
				return &HedgedHTTPResult{Index: res.index, Response: res.resp, Meta: finalizeMeta()}, nil
			}
			meta.Attempts[res.pos].Status = HedgeAttemptStatusError
			meta.ErrorCount++
			if res.err != nil {
				meta.Attempts[res.pos].Error = res.err.Error()
			}
			if !delayedStarted && policy.DelayedParallelCount > 0 && initial < maxAttempts {
				for i := initial; i < maxAttempts; i++ {
					startAttempt(i)
				}
				delayedStarted = true
				timer = nil
			}
		}
	}
	return nil, errors.New("all hedge attempts failed")
}
