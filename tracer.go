// Licensed to Elasticsearch B.V. under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Elasticsearch B.V. licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package apm

import (
	"bytes"
	"compress/zlib"
	"context"
	"io"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"go.elastic.co/apm/internal/apmconfig"
	"go.elastic.co/apm/internal/apmlog"
	"go.elastic.co/apm/internal/iochan"
	"go.elastic.co/apm/internal/ringbuffer"
	"go.elastic.co/apm/internal/wildcard"
	"go.elastic.co/apm/model"
	"go.elastic.co/apm/stacktrace"
	"go.elastic.co/apm/transport"
	"go.elastic.co/fastjson"
)

const (
	defaultPreContext     = 3
	defaultPostContext    = 3
	gracePeriodJitter     = 0.1 // +/- 10%
	tracerEventChannelCap = 1000
)

var (
	// DefaultTracer is the default global Tracer, set at package
	// initialization time, configured via environment variables.
	//
	// This will always be initialized to a non-nil value. If any
	// of the environment variables are invalid, the corresponding
	// errors will be logged to stderr and the default values will
	// be used instead.
	DefaultTracer *Tracer
)

func init() {
	var opts options
	opts.init(true)
	DefaultTracer = newTracer(opts)
}

type options struct {
	requestDuration       time.Duration
	metricsInterval       time.Duration
	maxSpans              int
	requestSize           int
	bufferSize            int
	metricsBufferSize     int
	sampler               Sampler
	sanitizedFieldNames   wildcard.Matchers
	disabledMetrics       wildcard.Matchers
	captureHeaders        bool
	captureBody           CaptureBodyMode
	spanFramesMinDuration time.Duration
	serviceName           string
	serviceVersion        string
	serviceEnvironment    string
	active                bool
}

func (opts *options) init(continueOnError bool) error {
	var errs []error
	failed := func(err error) bool {
		if err == nil {
			return false
		}
		errs = append(errs, err)
		return true
	}

	requestDuration, err := initialRequestDuration()
	if failed(err) {
		requestDuration = defaultAPIRequestTime
	}

	metricsInterval, err := initialMetricsInterval()
	if err != nil {
		metricsInterval = defaultMetricsInterval
		errs = append(errs, err)
	}

	requestSize, err := initialAPIRequestSize()
	if err != nil {
		requestSize = int(defaultAPIRequestSize)
		errs = append(errs, err)
	}

	bufferSize, err := initialAPIBufferSize()
	if err != nil {
		bufferSize = int(defaultAPIBufferSize)
		errs = append(errs, err)
	}

	metricsBufferSize, err := initialMetricsBufferSize()
	if err != nil {
		metricsBufferSize = int(defaultMetricsBufferSize)
		errs = append(errs, err)
	}

	maxSpans, err := initialMaxSpans()
	if failed(err) {
		maxSpans = defaultMaxSpans
	}

	sampler, err := initialSampler()
	if failed(err) {
		sampler = nil
	}

	captureHeaders, err := initialCaptureHeaders()
	if failed(err) {
		captureHeaders = defaultCaptureHeaders
	}

	captureBody, err := initialCaptureBody()
	if failed(err) {
		captureBody = CaptureBodyOff
	}

	spanFramesMinDuration, err := initialSpanFramesMinDuration()
	if failed(err) {
		spanFramesMinDuration = defaultSpanFramesMinDuration
	}

	active, err := initialActive()
	if failed(err) {
		active = true
	}

	if len(errs) != 0 && !continueOnError {
		return errs[0]
	}
	for _, err := range errs {
		log.Printf("[apm]: %s", err)
	}

	opts.requestDuration = requestDuration
	opts.metricsInterval = metricsInterval
	opts.requestSize = requestSize
	opts.bufferSize = bufferSize
	opts.metricsBufferSize = metricsBufferSize
	opts.maxSpans = maxSpans
	opts.sampler = sampler
	opts.sanitizedFieldNames = initialSanitizedFieldNames()
	opts.disabledMetrics = initialDisabledMetrics()
	opts.captureHeaders = captureHeaders
	opts.captureBody = captureBody
	opts.spanFramesMinDuration = spanFramesMinDuration
	opts.serviceName, opts.serviceVersion, opts.serviceEnvironment = initialService()
	opts.active = active
	return nil
}

// Tracer manages the sampling and sending of transactions to
// Elastic APM.
//
// Transactions are buffered until they are flushed (forcibly
// with a Flush call, or when the flush timer expires), or when
// the maximum transaction queue size is reached. Failure to
// send will be periodically retried. Once the queue limit has
// been reached, new transactions will replace older ones in
// the queue.
//
// Errors are sent as soon as possible, but will buffered and
// later sent in bulk if the tracer is busy, or otherwise cannot
// send to the server, e.g. due to network failure. There is
// a limit to the number of errors that will be buffered, and
// once that limit has been reached, new errors will be dropped
// until the queue is drained.
//
// The exported fields be altered or replaced any time up until
// any Tracer methods have been invoked.
type Tracer struct {
	Transport transport.Transport
	Service   struct {
		Name        string
		Version     string
		Environment string
	}

	process *model.Process
	system  *model.System

	active            int32
	bufferSize        int
	metricsBufferSize int
	closing           chan struct{}
	closed            chan struct{}
	forceFlush        chan chan<- struct{}
	forceSendMetrics  chan chan<- struct{}
	configCommands    chan tracerConfigCommand
	events            chan tracerEvent

	statsMu sync.Mutex
	stats   TracerStats

	maxSpansMu sync.RWMutex
	maxSpans   int

	spanFramesMinDurationMu sync.RWMutex
	spanFramesMinDuration   time.Duration

	samplerMu sync.RWMutex
	sampler   Sampler

	captureHeadersMu sync.RWMutex
	captureHeaders   bool

	captureBodyMu sync.RWMutex
	captureBody   CaptureBodyMode

	errorDataPool       sync.Pool
	spanDataPool        sync.Pool
	transactionDataPool sync.Pool
}

// NewTracer returns a new Tracer, using the default transport,
// initializing a Service with the specified name and version,
// or taking the service name and version from the environment
// if unspecified.
//
// If serviceName is empty, then the service name will be defined
// using the ELASTIC_APM_SERVER_NAME environment variable.
func NewTracer(serviceName, serviceVersion string) (*Tracer, error) {
	var opts options
	if err := opts.init(false); err != nil {
		return nil, err
	}
	if serviceName != "" {
		if err := validateServiceName(serviceName); err != nil {
			return nil, err
		}
		opts.serviceName = serviceName
		opts.serviceVersion = serviceVersion
	}
	return newTracer(opts), nil
}

func newTracer(opts options) *Tracer {
	t := &Tracer{
		Transport:             transport.Default,
		process:               &currentProcess,
		system:                &localSystem,
		closing:               make(chan struct{}),
		closed:                make(chan struct{}),
		forceFlush:            make(chan chan<- struct{}),
		forceSendMetrics:      make(chan chan<- struct{}),
		configCommands:        make(chan tracerConfigCommand),
		events:                make(chan tracerEvent, tracerEventChannelCap),
		active:                1,
		maxSpans:              opts.maxSpans,
		sampler:               opts.sampler,
		captureHeaders:        opts.captureHeaders,
		captureBody:           opts.captureBody,
		spanFramesMinDuration: opts.spanFramesMinDuration,
		bufferSize:            opts.bufferSize,
		metricsBufferSize:     opts.metricsBufferSize,
	}
	t.Service.Name = opts.serviceName
	t.Service.Version = opts.serviceVersion
	t.Service.Environment = opts.serviceEnvironment

	if !opts.active {
		t.active = 0
		close(t.closed)
		return t
	}

	go t.loop()
	t.configCommands <- func(cfg *tracerConfig) {
		cfg.metricsInterval = opts.metricsInterval
		cfg.requestDuration = opts.requestDuration
		cfg.requestSize = opts.requestSize
		cfg.sanitizedFieldNames = opts.sanitizedFieldNames
		cfg.disabledMetrics = opts.disabledMetrics
		cfg.preContext = defaultPreContext
		cfg.postContext = defaultPostContext
		cfg.metricsGatherers = []MetricsGatherer{newBuiltinMetricsGatherer(t)}
		if apmlog.DefaultLogger != nil {
			cfg.logger = apmlog.DefaultLogger
		}
	}
	return t
}

// tracerConfig holds the tracer's runtime configuration, which may be modified
// by sending a tracerConfigCommand to the tracer's configCommands channel.
type tracerConfig struct {
	requestSize             int
	requestDuration         time.Duration
	metricsInterval         time.Duration
	logger                  Logger
	metricsGatherers        []MetricsGatherer
	contextSetter           stacktrace.ContextSetter
	preContext, postContext int
	sanitizedFieldNames     wildcard.Matchers
	disabledMetrics         wildcard.Matchers
}

type tracerConfigCommand func(*tracerConfig)

// Close closes the Tracer, preventing transactions from being
// sent to the APM server.
func (t *Tracer) Close() {
	select {
	case <-t.closing:
	default:
		close(t.closing)
	}
	<-t.closed
}

// Flush waits for the Tracer to flush any transactions and errors it currently
// has queued to the APM server, the tracer is stopped, or the abort channel
// is signaled.
func (t *Tracer) Flush(abort <-chan struct{}) {
	flushed := make(chan struct{}, 1)
	select {
	case t.forceFlush <- flushed:
		select {
		case <-abort:
		case <-flushed:
		case <-t.closed:
		}
	case <-t.closed:
	}
}

// Active reports whether the tracer is active. If the tracer is inactive,
// no transactions or errors will be sent to the Elastic APM server.
func (t *Tracer) Active() bool {
	return atomic.LoadInt32(&t.active) == 1
}

// SetRequestDuration sets the maximum amount of time to keep a request open
// to the APM server for streaming data before closing the stream and starting
// a new request.
func (t *Tracer) SetRequestDuration(d time.Duration) {
	t.sendConfigCommand(func(cfg *tracerConfig) {
		cfg.requestDuration = d
	})
}

// SetMetricsInterval sets the metrics interval -- the amount of time in
// between metrics samples being gathered.
func (t *Tracer) SetMetricsInterval(d time.Duration) {
	t.sendConfigCommand(func(cfg *tracerConfig) {
		cfg.metricsInterval = d
	})
}

// SetContextSetter sets the stacktrace.ContextSetter to be used for
// setting stacktrace source context. If nil (which is the initial
// value), no context will be set.
func (t *Tracer) SetContextSetter(setter stacktrace.ContextSetter) {
	t.sendConfigCommand(func(cfg *tracerConfig) {
		cfg.contextSetter = setter
	})
}

// SetLogger sets the Logger to be used for logging the operation of
// the tracer.
//
// The tracer is initialized with a default logger configured with the
// environment variables ELASTIC_APM_LOG_FILE and ELASTIC_APM_LOG_LEVEL.
// Calling SetLogger will replace the default logger.
func (t *Tracer) SetLogger(logger Logger) {
	t.sendConfigCommand(func(cfg *tracerConfig) {
		cfg.logger = logger
	})
}

// SetSanitizedFieldNames sets the wildcard patterns that will be used to
// match cookie and form field names for sanitization. Fields matching any
// of the the supplied patterns will have their values redacted. If
// SetSanitizedFieldNames is called with no arguments, then no fields
// will be redacted.
func (t *Tracer) SetSanitizedFieldNames(patterns ...string) error {
	var matchers wildcard.Matchers
	if len(patterns) != 0 {
		matchers = make(wildcard.Matchers, len(patterns))
		for i, p := range patterns {
			matchers[i] = apmconfig.ParseWildcardPattern(p)
		}
	}
	t.sendConfigCommand(func(cfg *tracerConfig) {
		cfg.sanitizedFieldNames = matchers
	})
	return nil
}

// RegisterMetricsGatherer registers g for periodic (or forced) metrics
// gathering by t.
//
// RegisterMetricsGatherer returns a function which will deregister g.
// It may safely be called multiple times.
func (t *Tracer) RegisterMetricsGatherer(g MetricsGatherer) func() {
	// Wrap g in a pointer-to-struct, so we can safely compare.
	wrapped := &struct{ MetricsGatherer }{MetricsGatherer: g}
	t.sendConfigCommand(func(cfg *tracerConfig) {
		cfg.metricsGatherers = append(cfg.metricsGatherers, wrapped)
	})
	deregister := func(cfg *tracerConfig) {
		for i, g := range cfg.metricsGatherers {
			if g != wrapped {
				continue
			}
			cfg.metricsGatherers = append(cfg.metricsGatherers[:i], cfg.metricsGatherers[i+1:]...)
		}
	}
	var once sync.Once
	return func() {
		once.Do(func() {
			t.sendConfigCommand(deregister)
		})
	}
}

func (t *Tracer) sendConfigCommand(cmd tracerConfigCommand) {
	select {
	case t.configCommands <- cmd:
	case <-t.closing:
	case <-t.closed:
	}
}

// SetSampler sets the sampler the tracer. It is valid to pass nil,
// in which case all transactions will be sampled.
func (t *Tracer) SetSampler(s Sampler) {
	t.samplerMu.Lock()
	t.sampler = s
	t.samplerMu.Unlock()
}

// SetMaxSpans sets the maximum number of spans that will be added
// to a transaction before dropping spans. If set to a non-positive
// value, the number of spans is unlimited.
func (t *Tracer) SetMaxSpans(n int) {
	t.maxSpansMu.Lock()
	t.maxSpans = n
	t.maxSpansMu.Unlock()
}

// SetSpanFramesMinDuration sets the minimum duration for a span after which
// we will capture its stack frames.
func (t *Tracer) SetSpanFramesMinDuration(d time.Duration) {
	t.spanFramesMinDurationMu.Lock()
	t.spanFramesMinDuration = d
	t.spanFramesMinDurationMu.Unlock()
}

// SetCaptureHeaders enables or disables capturing of HTTP headers.
func (t *Tracer) SetCaptureHeaders(capture bool) {
	t.captureHeadersMu.Lock()
	t.captureHeaders = capture
	t.captureHeadersMu.Unlock()
}

// SetCaptureBody sets the HTTP request body capture mode.
func (t *Tracer) SetCaptureBody(mode CaptureBodyMode) {
	t.captureBodyMu.Lock()
	t.captureBody = mode
	t.captureBodyMu.Unlock()
}

// SendMetrics forces the tracer to gather and send metrics immediately,
// blocking until the metrics have been sent or the abort channel is
// signalled.
func (t *Tracer) SendMetrics(abort <-chan struct{}) {
	sent := make(chan struct{}, 1)
	select {
	case t.forceSendMetrics <- sent:
		select {
		case <-abort:
		case <-sent:
		case <-t.closed:
		}
	case <-t.closed:
	}
}

// Stats returns the current TracerStats. This will return the most
// recent values even after the tracer has been closed.
func (t *Tracer) Stats() TracerStats {
	t.statsMu.Lock()
	stats := t.stats
	t.statsMu.Unlock()
	return stats
}

func (t *Tracer) loop() {
	ctx, cancelContext := context.WithCancel(context.Background())
	defer cancelContext()
	defer close(t.closed)
	defer atomic.StoreInt32(&t.active, 0)

	var req iochan.ReadRequest
	var requestBuf bytes.Buffer
	var metadata []byte
	var gracePeriod time.Duration = -1
	var flushed chan<- struct{}
	var requestBufTransactions, requestBufSpans, requestBufErrors, requestBufMetricsets uint64
	zlibWriter, _ := zlib.NewWriterLevel(&requestBuf, zlib.BestSpeed)
	zlibFlushed := true
	zlibClosed := false
	iochanReader := iochan.NewReader()
	requestBytesRead := 0
	requestActive := false
	closeRequest := false
	flushRequest := false
	requestResult := make(chan error, 1)
	requestTimer := time.NewTimer(0)
	requestTimerActive := false
	if !requestTimer.Stop() {
		<-requestTimer.C
	}

	// Run another goroutine to perform the blocking requests,
	// communicating with the tracer loop to obtain stream data.
	sendStreamRequest := make(chan time.Duration)
	defer close(sendStreamRequest)
	go func() {
		jitterRand := rand.New(rand.NewSource(time.Now().UnixNano()))
		for gracePeriod := range sendStreamRequest {
			if gracePeriod > 0 {
				select {
				case <-time.After(jitterDuration(gracePeriod, jitterRand, gracePeriodJitter)):
				case <-ctx.Done():
				}
			}
			requestResult <- t.Transport.SendStream(ctx, iochanReader)
		}
	}()

	var stats TracerStats
	var metrics Metrics
	var sentMetrics chan<- struct{}
	var gatheringMetrics bool
	var metricsTimerStart time.Time
	metricsBuffer := ringbuffer.New(t.metricsBufferSize)
	gatheredMetrics := make(chan struct{}, 1)
	metricsTimer := time.NewTimer(0)
	if !metricsTimer.Stop() {
		<-metricsTimer.C
	}

	var cfg tracerConfig
	buffer := ringbuffer.New(t.bufferSize)
	buffer.Evicted = func(h ringbuffer.BlockHeader) {
		switch h.Tag {
		case errorBlockTag:
			stats.ErrorsDropped++
		case spanBlockTag:
			stats.SpansDropped++
		case transactionBlockTag:
			stats.TransactionsDropped++
		}
	}
	modelWriter := modelWriter{
		buffer:        buffer,
		metricsBuffer: metricsBuffer,
		cfg:           &cfg,
		stats:         &stats,
	}

	for {
		var gatherMetrics bool
		select {
		case <-t.closing:
			cancelContext() // informs transport that EOF is expected
			iochanReader.CloseRead(io.EOF)
			return
		case cmd := <-t.configCommands:
			oldMetricsInterval := cfg.metricsInterval
			cmd(&cfg)
			if !gatheringMetrics && cfg.metricsInterval != oldMetricsInterval {
				if metricsTimerStart.IsZero() {
					if cfg.metricsInterval > 0 {
						metricsTimer.Reset(cfg.metricsInterval)
						metricsTimerStart = time.Now()
					}
				} else {
					if cfg.metricsInterval <= 0 {
						metricsTimerStart = time.Time{}
						if !metricsTimer.Stop() {
							<-metricsTimer.C
						}
					} else {
						alreadyPassed := time.Since(metricsTimerStart)
						if alreadyPassed >= cfg.metricsInterval {
							metricsTimer.Reset(0)
						} else {
							metricsTimer.Reset(cfg.metricsInterval - alreadyPassed)
						}
					}
				}
			}
			continue
		case event := <-t.events:
			switch event.eventType {
			case transactionEvent:
				modelWriter.writeTransaction(event.tx.Transaction, event.tx.TransactionData)
			case spanEvent:
				modelWriter.writeSpan(event.span.Span, event.span.SpanData)
			case errorEvent:
				modelWriter.writeError(event.err)
				// Flush the buffer to transmit the error immediately.
				flushRequest = true
			}
		case <-requestTimer.C:
			requestTimerActive = false
			closeRequest = true
		case <-metricsTimer.C:
			metricsTimerStart = time.Time{}
			gatherMetrics = !gatheringMetrics
		case sentMetrics = <-t.forceSendMetrics:
			if !metricsTimerStart.IsZero() {
				if !metricsTimer.Stop() {
					<-metricsTimer.C
				}
				metricsTimerStart = time.Time{}
			}
			gatherMetrics = !gatheringMetrics
		case <-gatheredMetrics:
			modelWriter.writeMetrics(&metrics)
			gatheringMetrics = false
			flushRequest = true
			if cfg.metricsInterval > 0 {
				metricsTimerStart = time.Now()
				metricsTimer.Reset(cfg.metricsInterval)
			}
		case flushed = <-t.forceFlush:
			// Drain any objects buffered in the channels.
			for n := len(t.events); n > 0; n-- {
				event := <-t.events
				switch event.eventType {
				case transactionEvent:
					modelWriter.writeTransaction(event.tx.Transaction, event.tx.TransactionData)
				case spanEvent:
					modelWriter.writeSpan(event.span.Span, event.span.SpanData)
				case errorEvent:
					modelWriter.writeError(event.err)
				}
			}
			if !requestActive && buffer.Len() == 0 && metricsBuffer.Len() == 0 {
				flushed <- struct{}{}
				continue
			}
			closeRequest = true
		case req = <-iochanReader.C:
		case err := <-requestResult:
			if err != nil {
				stats.Errors.SendStream++
				gracePeriod = nextGracePeriod(gracePeriod)
				if cfg.logger != nil {
					logf := cfg.logger.Debugf
					if err, ok := err.(*transport.HTTPError); ok && err.Response.StatusCode == 404 {
						// 404 typically means the server is too old, meaning
						// the error is due to a misconfigured environment.
						logf = cfg.logger.Errorf
					}
					logf("request failed: %s (next request in ~%s)", err, gracePeriod)
				}
			} else {
				gracePeriod = -1 // Reset grace period after success.
				stats.TransactionsSent += requestBufTransactions
				stats.SpansSent += requestBufSpans
				stats.ErrorsSent += requestBufErrors
				if cfg.logger != nil {
					s := func(n uint64) string {
						if n != 1 {
							return "s"
						}
						return ""
					}
					cfg.logger.Debugf(
						"sent request with %d transaction%s, %d span%s, %d error%s, %d metricset%s",
						requestBufTransactions, s(requestBufTransactions),
						requestBufSpans, s(requestBufSpans),
						requestBufErrors, s(requestBufErrors),
						requestBufMetricsets, s(requestBufMetricsets),
					)
				}
			}
			if !stats.isZero() {
				t.statsMu.Lock()
				t.stats.accumulate(stats)
				t.statsMu.Unlock()
				stats = TracerStats{}
			}
			if sentMetrics != nil && requestBufMetricsets > 0 {
				sentMetrics <- struct{}{}
				sentMetrics = nil
			}
			if flushed != nil {
				flushed <- struct{}{}
				flushed = nil
			}
			if req.Buf != nil {
				// req will be canceled by CloseRead below.
				req.Buf = nil
			}
			iochanReader.CloseRead(io.EOF)
			iochanReader = iochan.NewReader()
			flushRequest = false
			closeRequest = false
			requestActive = false
			requestBytesRead = 0
			requestBuf.Reset()
			requestBufTransactions = 0
			requestBufSpans = 0
			requestBufErrors = 0
			requestBufMetricsets = 0
			if requestTimerActive {
				if !requestTimer.Stop() {
					<-requestTimer.C
				}
				requestTimerActive = false
			}
		}

		if !stats.isZero() {
			t.statsMu.Lock()
			t.stats.accumulate(stats)
			t.statsMu.Unlock()
			stats = TracerStats{}
		}

		if gatherMetrics {
			gatheringMetrics = true
			metrics.disabled = cfg.disabledMetrics
			t.gatherMetrics(ctx, cfg.metricsGatherers, &metrics, cfg.logger, gatheredMetrics)
			if cfg.logger != nil {
				cfg.logger.Debugf("gathering metrics")
			}
		}

		if !requestActive {
			if buffer.Len() == 0 && metricsBuffer.Len() == 0 {
				continue
			}
			sendStreamRequest <- gracePeriod
			if metadata == nil {
				metadata = t.jsonRequestMetadata()
			}
			zlibWriter.Reset(&requestBuf)
			zlibWriter.Write(metadata)
			zlibFlushed = false
			zlibClosed = false
			requestActive = true
			requestTimer.Reset(cfg.requestDuration)
			requestTimerActive = true
		}

		if !closeRequest || !zlibClosed {
			for requestBytesRead+requestBuf.Len() < cfg.requestSize {
				if metricsBuffer.Len() > 0 {
					if _, _, err := metricsBuffer.WriteBlockTo(zlibWriter); err == nil {
						requestBufMetricsets++
						zlibWriter.Write([]byte("\n"))
						zlibFlushed = false
						if sentMetrics != nil {
							// SendMetrics was called: close the request
							// off so we can inform the user when the
							// metrics have been processed.
							closeRequest = true
						}
					}
					continue
				}
				if buffer.Len() == 0 {
					break
				}
				if h, _, err := buffer.WriteBlockTo(zlibWriter); err == nil {
					switch h.Tag {
					case transactionBlockTag:
						requestBufTransactions++
					case spanBlockTag:
						requestBufSpans++
					case errorBlockTag:
						requestBufErrors++
					}
					zlibWriter.Write([]byte("\n"))
					zlibFlushed = false
				}
			}
			if !closeRequest {
				closeRequest = requestBytesRead+requestBuf.Len() >= cfg.requestSize
			}
		}
		if closeRequest {
			if !zlibClosed {
				zlibWriter.Close()
				zlibClosed = true
			}
		} else if flushRequest && !zlibFlushed {
			zlibWriter.Flush()
			flushRequest = false
			zlibFlushed = true
		}

		if req.Buf == nil || requestBuf.Len() == 0 {
			continue
		}
		const zlibHeaderLen = 2
		if requestBytesRead+requestBuf.Len() > zlibHeaderLen {
			n, err := requestBuf.Read(req.Buf)
			if closeRequest && err == nil && requestBuf.Len() == 0 {
				err = io.EOF
			}
			req.Respond(n, err)
			req.Buf = nil
			if n > 0 {
				requestBytesRead += n
			}
		}
	}
}

// jsonRequestMetadata returns a JSON-encoded metadata object that features
// at the head of every request body. This is called exactly once, when the
// first request is made.
func (t *Tracer) jsonRequestMetadata() []byte {
	var json fastjson.Writer
	service := makeService(t.Service.Name, t.Service.Version, t.Service.Environment)
	json.RawString(`{"metadata":{`)
	json.RawString(`"system":`)
	t.system.MarshalFastJSON(&json)
	json.RawString(`,"process":`)
	t.process.MarshalFastJSON(&json)
	json.RawString(`,"service":`)
	service.MarshalFastJSON(&json)
	if len(globalLabels) > 0 {
		json.RawString(`,"labels":`)
		globalLabels.MarshalFastJSON(&json)
	}
	json.RawString("}}\n")
	return json.Bytes()
}

// gatherMetrics gathers metrics from each of the registered
// metrics gatherers. Once all gatherers have returned, a value
// will be sent on the "gathered" channel.
func (t *Tracer) gatherMetrics(ctx context.Context, gatherers []MetricsGatherer, m *Metrics, l Logger, gathered chan<- struct{}) {
	timestamp := model.Time(time.Now().UTC())
	var group sync.WaitGroup
	for _, g := range gatherers {
		group.Add(1)
		go func(g MetricsGatherer) {
			defer group.Done()
			gatherMetrics(ctx, g, m, l)
		}(g)
	}
	go func() {
		group.Wait()
		for _, m := range m.metrics {
			m.Timestamp = timestamp
		}
		gathered <- struct{}{}
	}()
}

type tracerEventType int

const (
	transactionEvent tracerEventType = iota
	spanEvent
	errorEvent
)

type tracerEvent struct {
	eventType tracerEventType

	// err is set only if eventType == errorEvent.
	err *ErrorData

	// tx is set only if eventType == transactionEvent.
	tx struct {
		*Transaction
		// Transaction.TransactionData is nil at the
		// point tracerEvent is created (to signify
		// that the transaction is ended), so we pass
		// it along side.
		*TransactionData
	}

	// span is set only if eventType == spanEvent.
	span struct {
		*Span
		// Span.SpanData is nil at the point tracerEvent
		// is created (to signify that the span is ended),
		// so we pass it along side.
		*SpanData
	}
}
