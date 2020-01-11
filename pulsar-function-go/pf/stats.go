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
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	io_prometheus_client "github.com/prometheus/client_model/go"
	//"strings"
	//"github.com/prometheus/common/expfmt"
	//"time"
)

var metrics_label_names = []string{"tenant", "namespace", "name", "instance_id", "cluster", "fqfn"}
var exception_label_names = []string{"error", "ts"}
var exception_metrics_label_names = append(metrics_label_names, exception_label_names...)

const (
	PULSAR_FUNCTION_METRICS_PREFIX = "pulsar_function_"
	USER_METRIC_PREFIX             = "user_metric_"

	TOTAL_SUCCESSFULLY_PROCESSED = "processed_successfully_total"
	TOTAL_SYSTEM_EXCEPTIONS      = "system_exceptions_total"
	TOTAL_USER_EXCEPTIONS        = "user_exceptions_total"
	PROCESS_LATENCY_MS           = "process_latency_ms"
	LAST_INVOCATION              = "last_invocation"
	TOTAL_RECEIVED               = "received_total"

	TOTAL_SUCCESSFULLY_PROCESSED_1min = "processed_successfully_total_1min"
	TOTAL_SYSTEM_EXCEPTIONS_1min      = "system_exceptions_total_1min"
	TOTAL_USER_EXCEPTIONS_1min        = "user_exceptions_total_1min"
	PROCESS_LATENCY_MS_1min           = "process_latency_ms_1min"
	TOTAL_RECEIVED_1min               = "received_total_1min"
)

// Declare Prometheus
var stat_total_processed_successfully = prometheus.NewGaugeVec(
	prometheus.GaugeOpts{
		Name: PULSAR_FUNCTION_METRICS_PREFIX + TOTAL_SUCCESSFULLY_PROCESSED,
		Help: "Total number of messages processed successfully."},
	metrics_label_names)
var stat_total_sys_exceptions = prometheus.NewGaugeVec(
	prometheus.GaugeOpts{
		Name: PULSAR_FUNCTION_METRICS_PREFIX + TOTAL_SYSTEM_EXCEPTIONS,
		Help: "Total number of system exceptions."},
	metrics_label_names)
var stat_total_user_exceptions = prometheus.NewGaugeVec(
	prometheus.GaugeOpts{
		Name: PULSAR_FUNCTION_METRICS_PREFIX + TOTAL_USER_EXCEPTIONS,
		Help: "Total number of user exceptions."},
	metrics_label_names)

var stat_process_latency_ms = prometheus.NewSummaryVec(
	prometheus.SummaryOpts{
		Name: PULSAR_FUNCTION_METRICS_PREFIX + PROCESS_LATENCY_MS,
		Help: "Process latency in milliseconds."}, metrics_label_names)

var stat_last_invocation = prometheus.NewGaugeVec(
	prometheus.GaugeOpts{
		Name: PULSAR_FUNCTION_METRICS_PREFIX + LAST_INVOCATION,
		Help: "The timestamp of the last invocation of the function."}, metrics_label_names)

var stat_total_received = prometheus.NewGaugeVec(
	prometheus.GaugeOpts{
		Name: PULSAR_FUNCTION_METRICS_PREFIX + TOTAL_RECEIVED,
		Help: "Total number of messages received from source."}, metrics_label_names)

// 1min windowed metrics
var stat_total_processed_successfully_1min = prometheus.NewGaugeVec(
	prometheus.GaugeOpts{
		Name: PULSAR_FUNCTION_METRICS_PREFIX + TOTAL_SUCCESSFULLY_PROCESSED_1min,
		Help: "Total number of messages processed successfully in the last 1 minute."}, metrics_label_names)
var stat_total_sys_exceptions_1min = prometheus.NewGaugeVec(
	prometheus.GaugeOpts{
		Name: PULSAR_FUNCTION_METRICS_PREFIX + TOTAL_SYSTEM_EXCEPTIONS_1min,
		Help: "Total number of system exceptions in the last 1 minute."},
	metrics_label_names)
var stat_total_user_exceptions_1min = prometheus.NewGaugeVec(
	prometheus.GaugeOpts{
		Name: PULSAR_FUNCTION_METRICS_PREFIX + TOTAL_USER_EXCEPTIONS_1min,
		Help: "Total number of user exceptions in the last 1 minute."},
	metrics_label_names)

var stat_process_latency_ms_1min = prometheus.NewSummaryVec(
	prometheus.SummaryOpts{
		Name: PULSAR_FUNCTION_METRICS_PREFIX + PROCESS_LATENCY_MS_1min,
		Help: "Process latency in milliseconds in the last 1 minute."}, metrics_label_names)

var stat_total_received_1min = prometheus.NewGaugeVec(
	prometheus.GaugeOpts{
		Name: PULSAR_FUNCTION_METRICS_PREFIX + TOTAL_RECEIVED_1min,
		Help: "Total number of messages received from source in the last 1 minute."}, metrics_label_names)

// exceptions
var user_exceptions = prometheus.NewGaugeVec(
	prometheus.GaugeOpts{
		Name: PULSAR_FUNCTION_METRICS_PREFIX + "user_exception",
		Help: "Exception from user code."}, exception_metrics_label_names)

var system_exceptions = prometheus.NewGaugeVec(
	prometheus.GaugeOpts{
		Name: PULSAR_FUNCTION_METRICS_PREFIX + "system_exception",
		Help: "Exception from system code."}, exception_metrics_label_names)

var reg *prometheus.Registry

func init() {
	reg = prometheus.NewRegistry()
	reg.MustRegister(stat_total_processed_successfully)
	reg.MustRegister(stat_total_sys_exceptions)
	reg.MustRegister(stat_total_user_exceptions)
	reg.MustRegister(stat_process_latency_ms)
	reg.MustRegister(stat_last_invocation)
	reg.MustRegister(stat_total_received)
	reg.MustRegister(stat_total_processed_successfully_1min)
	reg.MustRegister(stat_total_sys_exceptions_1min)
	reg.MustRegister(stat_total_user_exceptions_1min)
	reg.MustRegister(stat_process_latency_ms_1min)
	reg.MustRegister(stat_total_received_1min)
	reg.MustRegister(user_exceptions)
	reg.MustRegister(system_exceptions)

}

type LatestException struct {
	exception error
	timestamp int64
}

// Be sure to use the constructor method: NewStatWithLabelValues
type statWithLabelValues struct {
	_stat_total_processed_successfully      prometheus.Gauge
	_stat_total_sys_exceptions              prometheus.Gauge
	_stat_total_user_exceptions             prometheus.Gauge
	_stat_process_latency_ms                prometheus.Observer
	_stat_last_invocation                   prometheus.Gauge
	_stat_total_received                    prometheus.Gauge
	_stat_total_processed_successfully_1min prometheus.Gauge
	_stat_total_sys_exceptions_1min         prometheus.Gauge
	_stat_total_user_exceptions_1min        prometheus.Gauge
	//_stat_process_latency_ms_1min prometheus.Observer
	_stat_total_received_1min prometheus.Gauge
	latest_user_exception     []LatestException
	latest_sys_exception      []LatestException
	process_start_time        int64
	metrics_labels            []string
}

func NewStatWithLabelValues(metrics_labels ...string) statWithLabelValues {
	// as optimization
	var _stat_total_processed_successfully = stat_total_processed_successfully.WithLabelValues(metrics_labels...)
	var _stat_total_sys_exceptions = stat_total_sys_exceptions.WithLabelValues(metrics_labels...)
	var _stat_total_user_exceptions = stat_total_user_exceptions.WithLabelValues(metrics_labels...)
	var _stat_process_latency_ms = stat_process_latency_ms.WithLabelValues(metrics_labels...)
	var _stat_last_invocation = stat_last_invocation.WithLabelValues(metrics_labels...)
	var _stat_total_received = stat_total_received.WithLabelValues(metrics_labels...)
	var _stat_total_processed_successfully_1min = stat_total_processed_successfully_1min.WithLabelValues(metrics_labels...)
	var _stat_total_sys_exceptions_1min = stat_total_sys_exceptions_1min.WithLabelValues(metrics_labels...)
	var _stat_total_user_exceptions_1min = stat_total_user_exceptions_1min.WithLabelValues(metrics_labels...)
	//var _stat_process_latency_ms_1min = stat_process_latency_ms_1min.WithLabelValues(metrics_labels...)
	var _stat_total_received_1min = stat_total_received_1min.WithLabelValues(metrics_labels...)

	statObj := statWithLabelValues{
		_stat_total_processed_successfully,
		_stat_total_sys_exceptions,
		_stat_total_user_exceptions,
		_stat_process_latency_ms,
		_stat_last_invocation,
		_stat_total_received,
		_stat_total_processed_successfully_1min,
		_stat_total_sys_exceptions_1min,
		_stat_total_user_exceptions_1min,
		//_stat_process_latency_ms_1min,
		_stat_total_received_1min,
		[]LatestException{},
		[]LatestException{},
		0,
		metrics_labels,
	}
	return statObj
}

func filter(ss []*io_prometheus_client.MetricFamily, test func(*io_prometheus_client.MetricFamily) bool) (ret []*io_prometheus_client.MetricFamily) {
	for _, s := range ss {
		if test(s) {
			ret = append(ret, s)
		}
	}
	return
}

/*
func filter(ss []*io_prometheus_client.Metric, test func(*io_prometheus_client.Metric) bool) (ret []*io_prometheus_client.Metric) {
	for _, s := range ss {
		if test(s) {
			ret = append(ret, s)
		}
	}
	return
}*/
func getFirstMatch(metrics []*io_prometheus_client.Metric, test func(*io_prometheus_client.LabelPair) bool) *io_prometheus_client.Metric {
	for _, met := range metrics {
		for _, lbl := range met.Label {
			if test(lbl) {
				return met
			}
		}
	}
	return nil
}
func getFirstMetricWithMatchingLabel(metrics []*io_prometheus_client.Metric, fqfn string) *io_prometheus_client.Metric {
	for _, met := range metrics {
		for _, lbl := range met.Label {
			if *lbl.Name == "fqfn" && *lbl.Value == fqfn {
				return met
			}
		}
	}
	return nil
}

func (stat *statWithLabelValues) set_last_invocation() {
	now := time.Now()
	stat._stat_last_invocation.Set(float64(now.UnixNano()))
}

func (stat *statWithLabelValues) process_time_start() {
	now := time.Now()
	stat.process_start_time = now.UnixNano()
}

func (stat *statWithLabelValues) process_time_end() {
	if stat.process_start_time != 0 {
		now := time.Now()
		duration := now.UnixNano() - stat.process_start_time
		stat._stat_process_latency_ms.Observe(float64(duration))
		//stat._stat_process_latency_ms_1min.Observe(float64(duration))
	}
}

func (stat *statWithLabelValues) incr_total_user_exceptions(err error) {
	stat._stat_total_user_exceptions.Inc()
	stat._stat_total_user_exceptions_1min.Inc()
	stat.add_user_exception(err)
}

func (stat *statWithLabelValues) add_user_exception(err error) {
	now := time.Now()
	ts := now.UnixNano()
	errorTs := LatestException{err, ts}
	stat.latest_user_exception = append(stat.latest_user_exception, errorTs)
	if len(stat.latest_user_exception) > 10 {
		stat.latest_user_exception = stat.latest_user_exception[1:]
	}
	// report exception via prometheus
	stat.report_user_exception_prometheus(err, ts)
}

//@limits(calls=5, period=60)
func (stat *statWithLabelValues) report_user_exception_prometheus(exception error, ts int64) {
	errorTs := []string{exception.Error(), strconv.FormatInt(ts, 10)}
	exception_metric_labels := append(stat.metrics_labels, errorTs...)
	user_exceptions.WithLabelValues(exception_metric_labels...).Set(1.0)
}

func (stat *statWithLabelValues) incr_total_processed_successfully() {
	stat._stat_total_processed_successfully.Inc()
	stat._stat_total_processed_successfully_1min.Inc()
}

func (stat *statWithLabelValues) incr_total_sys_exceptions(exception error) {
	stat._stat_total_sys_exceptions.Inc()
	stat._stat_total_sys_exceptions_1min.Inc()
	stat.add_sys_exception(exception)
}

func (stat *statWithLabelValues) add_sys_exception(exception error) {
	now := time.Now()
	ts := now.UnixNano()
	errorTs := LatestException{exception, ts}
	stat.latest_sys_exception = append(stat.latest_sys_exception, errorTs)
	if len(stat.latest_sys_exception) > 10 {
		stat.latest_sys_exception = stat.latest_sys_exception[1:]
	}
	// report exception via prometheus
	stat.report_system_exception_prometheus(exception, ts)
}

//@limits(calls=5, period=60)
func (stat *statWithLabelValues) report_system_exception_prometheus(exception error, ts int64) {
	errorTs := []string{exception.Error(), strconv.FormatInt(ts, 10)}
	exception_metric_labels := append(stat.metrics_labels, errorTs...)
	system_exceptions.WithLabelValues(exception_metric_labels...).Set(1.0)
}

func (stat *statWithLabelValues) incr_total_received() {
	stat._stat_total_received.Inc()
	stat._stat_total_received_1min.Inc()
}

func (stat *statWithLabelValues) reset() {
	stat._stat_total_processed_successfully_1min.Set(0.0)
	stat._stat_total_user_exceptions_1min.Set(0.0)
	stat._stat_total_sys_exceptions_1min.Set(0.0)
	//stat._stat_process_latency_ms_1min._sum.set(0.0)
	//stat._stat_process_latency_ms_1min._count.set(0.0)
	stat._stat_total_received_1min.Set(0.0)
}

/*
// start time for windowed metrics
util.FixedTimer(60, reset, name="windowed-metrics-timer").start()
*/
