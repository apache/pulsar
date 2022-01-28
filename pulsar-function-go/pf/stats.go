//
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
//

package pf

import (
	"fmt"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	log "github.com/apache/pulsar/pulsar-function-go/logutil"
	"github.com/prometheus/client_golang/prometheus"
	prometheus_client "github.com/prometheus/client_model/go"
)

var (
	metricsLabelNames          = []string{"tenant", "namespace", "name", "instance_id", "cluster", "fqfn"}
	exceptionLabelNames        = []string{"error"}
	exceptionMetricsLabelNames = append(metricsLabelNames, exceptionLabelNames...)
)

const (
	PulsarFunctionMetricsPrefix = "pulsar_function_"

	TotalSuccessfullyProcessed = "processed_successfully_total"
	TotalSystemExceptions      = "system_exceptions_total"
	TotalUserExceptions        = "user_exceptions_total"
	ProcessLatencyMs           = "process_latency_ms"
	LastInvocation             = "last_invocation"
	TotalReceived              = "received_total"

	TotalSuccessfullyProcessed1min = "processed_successfully_total_1min"
	TotalSystemExceptions1min      = "system_exceptions_total_1min"
	TotalUserExceptions1min        = "user_exceptions_total_1min"
	ProcessLatencyMs1min           = "process_latency_ms_1min"
	TotalReceived1min              = "received_total_1min"
)

// Declare Prometheus
var (
	statTotalProcessedSuccessfully = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: PulsarFunctionMetricsPrefix + TotalSuccessfullyProcessed,
			Help: "Total number of messages processed successfully."},
		metricsLabelNames)
	statTotalSysExceptions = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: PulsarFunctionMetricsPrefix + TotalSystemExceptions,
			Help: "Total number of system exceptions."},
		metricsLabelNames)
	statTotalUserExceptions = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: PulsarFunctionMetricsPrefix + TotalUserExceptions,
			Help: "Total number of user exceptions."},
		metricsLabelNames)

	statProcessLatencyMs = prometheus.NewSummaryVec(
		prometheus.SummaryOpts{
			Name: PulsarFunctionMetricsPrefix + ProcessLatencyMs,
			Help: "Process latency in milliseconds."}, metricsLabelNames)

	statLastInvocation = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: PulsarFunctionMetricsPrefix + LastInvocation,
			Help: "The timestamp of the last invocation of the function."}, metricsLabelNames)

	statTotalReceived = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: PulsarFunctionMetricsPrefix + TotalReceived,
			Help: "Total number of messages received from source."}, metricsLabelNames)

	// 1min windowed metrics
	statTotalProcessedSuccessfully1min = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: PulsarFunctionMetricsPrefix + TotalSuccessfullyProcessed1min,
			Help: "Total number of messages processed successfully in the last 1 minute."}, metricsLabelNames)
	statTotalSysExceptions1min = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: PulsarFunctionMetricsPrefix + TotalSystemExceptions1min,
			Help: "Total number of system exceptions in the last 1 minute."},
		metricsLabelNames)
	statTotalUserExceptions1min = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: PulsarFunctionMetricsPrefix + TotalUserExceptions1min,
			Help: "Total number of user exceptions in the last 1 minute."},
		metricsLabelNames)

	statProcessLatencyMs1min = prometheus.NewSummaryVec(
		prometheus.SummaryOpts{
			Name: PulsarFunctionMetricsPrefix + ProcessLatencyMs1min,
			Help: "Process latency in milliseconds in the last 1 minute."}, metricsLabelNames)

	statTotalReceived1min = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: PulsarFunctionMetricsPrefix + TotalReceived1min,
			Help: "Total number of messages received from source in the last 1 minute."}, metricsLabelNames)

	userExceptions = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: PulsarFunctionMetricsPrefix + "user_exception",
			Help: "Exception from user code."}, exceptionMetricsLabelNames)

	systemExceptions = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: PulsarFunctionMetricsPrefix + "system_exception",
			Help: "Exception from system code."}, exceptionMetricsLabelNames)
)

type MetricsServicer struct {
	goInstance *goInstance
	server     *http.Server
}

var reg *prometheus.Registry

func init() {
	reg = prometheus.NewRegistry()
	reg.MustRegister(statTotalProcessedSuccessfully)
	reg.MustRegister(statTotalSysExceptions)
	reg.MustRegister(statTotalUserExceptions)
	reg.MustRegister(statProcessLatencyMs)
	reg.MustRegister(statLastInvocation)
	reg.MustRegister(statTotalReceived)
	reg.MustRegister(statTotalProcessedSuccessfully1min)
	reg.MustRegister(statTotalSysExceptions1min)
	reg.MustRegister(statTotalUserExceptions1min)
	reg.MustRegister(statProcessLatencyMs1min)
	reg.MustRegister(statTotalReceived1min)
	reg.MustRegister(userExceptions)
	reg.MustRegister(systemExceptions)

}

type LatestException struct {
	exception error
	timestamp int64
}

// Be sure to use the constructor method: NewStatWithLabelValues
type StatWithLabelValues struct {
	statTotalProcessedSuccessfully     prometheus.Gauge
	statTotalSysExceptions             prometheus.Gauge
	statTotalUserExceptions            prometheus.Gauge
	statProcessLatencyMs               prometheus.Observer
	statLastInvocation                 prometheus.Gauge
	statTotalReceived                  prometheus.Gauge
	statTotalProcessedSuccessfully1min prometheus.Gauge
	statTotalSysExceptions1min         prometheus.Gauge
	statTotalUserExceptions1min        prometheus.Gauge
	statTotalReceived1min              prometheus.Gauge
	latestUserException                []LatestException
	latestSysException                 []LatestException
	processStartTime                   int64
	metricsLabels                      []string
}

func NewStatWithLabelValues(metricsLabels ...string) StatWithLabelValues {
	// as optimization
	var statTotalProcessedSuccessfully = statTotalProcessedSuccessfully.WithLabelValues(metricsLabels...)
	var statTotalSysExceptions = statTotalSysExceptions.WithLabelValues(metricsLabels...)
	var statTotalUserExceptions = statTotalUserExceptions.WithLabelValues(metricsLabels...)
	var statProcessLatencyMs = statProcessLatencyMs.WithLabelValues(metricsLabels...)
	var statLastInvocation = statLastInvocation.WithLabelValues(metricsLabels...)
	var statTotalReceived = statTotalReceived.WithLabelValues(metricsLabels...)
	var statTotalProcessedSuccessfully1min = statTotalProcessedSuccessfully1min.WithLabelValues(metricsLabels...)
	var statTotalSysExceptions1min = statTotalSysExceptions1min.WithLabelValues(metricsLabels...)
	var statTotalUserExceptions1min = statTotalUserExceptions1min.WithLabelValues(metricsLabels...)
	//var _stat_process_latency_ms_1min = stat_process_latency_ms_1min.WithLabelValues(metrics_labels...)
	var statTotalReceived1min = statTotalReceived1min.WithLabelValues(metricsLabels...)

	statObj := StatWithLabelValues{
		statTotalProcessedSuccessfully,
		statTotalSysExceptions,
		statTotalUserExceptions,
		statProcessLatencyMs,
		statLastInvocation,
		statTotalReceived,
		statTotalProcessedSuccessfully1min,
		statTotalSysExceptions1min,
		statTotalUserExceptions1min,
		statTotalReceived1min,
		[]LatestException{},
		[]LatestException{},
		0,
		metricsLabels,
	}
	return statObj
}

func filter(
	ss []*prometheus_client.MetricFamily,
	test func(*prometheus_client.MetricFamily) bool) (ret []*prometheus_client.MetricFamily) {
	for _, s := range ss {
		if test(s) {
			ret = append(ret, s)
		}
	}
	return
}

func getFirstMatch(
	metrics []*prometheus_client.Metric,
	test func(*prometheus_client.LabelPair) bool) *prometheus_client.Metric {
	for _, met := range metrics {
		for _, lbl := range met.Label {
			if test(lbl) {
				return met
			}
		}
	}
	return nil
}

func (stat *StatWithLabelValues) setLastInvocation() {
	now := time.Now()
	stat.statLastInvocation.Set(float64(now.UnixNano()))
}

func (stat *StatWithLabelValues) processTimeStart() {
	now := time.Now()
	stat.processStartTime = now.UnixNano()
}

func (stat *StatWithLabelValues) processTimeEnd() {
	if stat.processStartTime != 0 {
		now := time.Now()
		duration := now.UnixNano() - stat.processStartTime
		stat.statProcessLatencyMs.Observe(float64(duration) / 1e6)
	}
}

func (stat *StatWithLabelValues) incrTotalUserExceptions(err error) {
	stat.statTotalUserExceptions.Inc()
	stat.statTotalUserExceptions1min.Inc()
	stat.addUserException(err)
}

func (stat *StatWithLabelValues) addUserException(err error) {
	now := time.Now()
	ts := now.UnixNano()
	errorTs := LatestException{err, ts}
	stat.latestUserException = append(stat.latestUserException, errorTs)
	if len(stat.latestUserException) > 10 {
		stat.latestUserException = stat.latestUserException[1:]
	}
	// report exception via prometheus
	stat.reportUserExceptionPrometheus(err)
}

//@limits(calls=5, period=60)
func (stat *StatWithLabelValues) reportUserExceptionPrometheus(exception error) {
	errorTs := []string{exception.Error()}
	exceptionMetricLabels := append(stat.metricsLabels, errorTs...)
	userExceptions.WithLabelValues(exceptionMetricLabels...).Set(1.0)
}

func (stat *StatWithLabelValues) incrTotalProcessedSuccessfully() {
	stat.statTotalProcessedSuccessfully.Inc()
	stat.statTotalProcessedSuccessfully1min.Inc()
}

func (stat *StatWithLabelValues) incrTotalSysExceptions(exception error) {
	stat.statTotalSysExceptions.Inc()
	stat.statTotalSysExceptions1min.Inc()
	stat.addSysException(exception)
}

func (stat *StatWithLabelValues) addSysException(exception error) {
	now := time.Now()
	ts := now.UnixNano()
	errorTs := LatestException{exception, ts}
	stat.latestSysException = append(stat.latestSysException, errorTs)
	if len(stat.latestSysException) > 10 {
		stat.latestSysException = stat.latestSysException[1:]
	}
	// report exception via prometheus
	stat.reportSystemExceptionPrometheus(exception)
}

//@limits(calls=5, period=60)
func (stat *StatWithLabelValues) reportSystemExceptionPrometheus(exception error) {
	errorTs := []string{exception.Error()}
	exceptionMetricLabels := append(stat.metricsLabels, errorTs...)
	systemExceptions.WithLabelValues(exceptionMetricLabels...).Set(1.0)
}

func (stat *StatWithLabelValues) incrTotalReceived() {
	stat.statTotalReceived.Inc()
	stat.statTotalReceived1min.Inc()
}

func (stat *StatWithLabelValues) reset() {
	stat.statTotalProcessedSuccessfully1min.Set(0.0)
	stat.statTotalUserExceptions1min.Set(0.0)
	stat.statTotalSysExceptions1min.Set(0.0)
	stat.statTotalReceived1min.Set(0.0)
}

func NewMetricsServicer(goInstance *goInstance) *MetricsServicer {
	serveMux := http.NewServeMux()
	pHandler := promhttp.HandlerFor(
		reg,
		promhttp.HandlerOpts{
			EnableOpenMetrics: true,
		},
	)
	serveMux.Handle("/", pHandler)
	serveMux.Handle("/metrics", pHandler)
	server := &http.Server{
		Addr:    fmt.Sprintf(":%d", goInstance.context.GetMetricsPort()),
		Handler: serveMux,
	}
	return &MetricsServicer{
		goInstance,
		server,
	}
}

func (s *MetricsServicer) serve() {
	go func() {
		// create a listener on metrics port
		log.Infof("Starting metrics server on port %d", s.goInstance.context.GetMetricsPort())
		err := s.server.ListenAndServe()
		if err != nil {
			log.Fatalf("failed to start metrics server: %v", err)
		}
	}()
}

func (s *MetricsServicer) close() {
	err := s.server.Close()
	if err != nil {
		log.Fatalf("failed to close metrics server: %v", err)
	}
}
