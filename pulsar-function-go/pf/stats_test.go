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
	"io/ioutil"
	"math"
	"net/http"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"

	prometheus_client "github.com/prometheus/client_model/go"
)

func TestExampleSummaryVec(t *testing.T) {

	temps := prometheus.NewSummaryVec(
		prometheus.SummaryOpts{
			Name:       "pond_temperature_celsius",
			Help:       "The temperature of the frog pond.",
			Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.99: 0.001},
		},
		[]string{"species"},
	)
	// Simulate some observations.
	for i := 0; i < 1000; i++ {
		temps.WithLabelValues("litoria-caerulea").Observe(30 + math.Floor(120*math.Sin(float64(i)*0.1))/10)
		temps.WithLabelValues("lithobates-catesbeianus").Observe(32 + math.Floor(100*math.Cos(float64(i)*0.11))/10)
	}

	// Create a Summary without any observations.
	temps.WithLabelValues("leiopelma-hochstetteri")

	// Just for demonstration, let's check the state of the summary vector
	// by registering it with a custom registry and then let it collect the
	// metrics.
	reg := prometheus.NewRegistry()
	reg.MustRegister(temps)

	metricFamilies, err := reg.Gather()
	if err != nil || len(metricFamilies) != 1 {
		t.Fatal("unexpected behavior of custom test registry")
	}
	match := func(vect *prometheus_client.MetricFamily) bool {
		return *vect.Name == "pond_temperature_celsius"
	}
	filteredMetricFamilies := filter(metricFamilies, match)

	if len(filteredMetricFamilies) > 1 {
		t.Fatal("Too many metric families")
	}
	// Then, we need to filter the metrics in the family to one that matches our label.
	expectedValue := "name: \"pond_temperature_celsius\"\n" +
		"help: \"The temperature of the frog pond.\"\n" +
		"type: SUMMARY\n" +
		"metric: <\n" +
		"  label: <\n" +
		"    name: \"species\"\n" +
		"    value: \"leiopelma-hochstetteri\"\n" +
		"  >\n" +
		"  summary: <\n" +
		"    sample_count: 0\n" +
		"    sample_sum: 0\n" +
		"    quantile: <\n" +
		"      quantile: 0.5\n" +
		"      value: nan\n" +
		"    >\n" +
		"    quantile: <\n" +
		"      quantile: 0.9\n" +
		"      value: nan\n" +
		"    >\n" +
		"    quantile: <\n" +
		"      quantile: 0.99\n" +
		"      value: nan\n" +
		"    >\n" +
		"  >\n" +
		">\n" +
		"metric: <\n" +
		"  label: <\n" +
		"    name: \"species\"\n" +
		"    value: \"lithobates-catesbeianus\"\n" +
		"  >\n" +
		"  summary: <\n" +
		"    sample_count: 1000\n" +
		"    sample_sum: 31956.100000000017\n" +
		"    quantile: <\n" +
		"      quantile: 0.5\n" +
		"      value: 32.4\n" +
		"    >\n" +
		"    quantile: <\n" +
		"      quantile: 0.9\n" +
		"      value: 41.4\n" +
		"    >\n" +
		"    quantile: <\n" +
		"      quantile: 0.99\n" +
		"      value: 41.9\n" +
		"    >\n" +
		"  >\n" +
		">\n" +
		"metric: <\n" +
		"  label: <\n" +
		"    name: \"species\"\n" +
		"    value: \"litoria-caerulea\"\n" +
		"  >\n" +
		"  summary: <\n" +
		"    sample_count: 1000\n" +
		"    sample_sum: 29969.50000000001\n" +
		"    quantile: <\n" +
		"      quantile: 0.5\n" +
		"      value: 31.1\n" +
		"    >\n" +
		"    quantile: <\n" +
		"      quantile: 0.9\n" +
		"      value: 41.3\n" +
		"    >\n" +
		"    quantile: <\n" +
		"      quantile: 0.99\n" +
		"      value: 41.9\n" +
		"    >\n" +
		"  >\n" +
		">\n"
	assert.Equal(t, expectedValue, proto.MarshalTextString(metricFamilies[0]))
}
func TestExampleSummaryVec_Pulsar(t *testing.T) {
	_statProcessLatencyMs1 := prometheus.NewSummaryVec(
		prometheus.SummaryOpts{
			Name: "pulsar_function_process_latency_ms",
			Help: "Process latency in milliseconds."}, metricsLabelNames)

	metricsLabels := []string{"test-tenant", "test-tenant/test-namespace", "test-name", "1234", "test-cluster",
		"test-tenant/test-namespace/test-name"}
	// 1234 is instanceId
	statProcessLatencyMs := _statProcessLatencyMs1.WithLabelValues(metricsLabels...)

	// Simulate some observations.
	for i := 0; i < 1000; i++ {
		statProcessLatencyMs.Observe(30 + math.Floor(120*math.Sin(float64(i)*0.1))/10)
		statProcessLatencyMs.Observe(32 + math.Floor(100*math.Cos(float64(i)*0.11))/10)
	}

	// Just for demonstration, let's check the state of the summary vector
	// by registering it with a custom registry and then let it collect the
	// metrics.
	reg := prometheus.NewRegistry()
	reg.MustRegister(_statProcessLatencyMs1)

	metricFamilies, err := reg.Gather()
	if err != nil || len(metricFamilies) != 1 {
		t.Fatal("unexpected behavior of custom test registry")
	}
	matchFamilyFunc := func(vect *prometheus_client.MetricFamily) bool {
		return *vect.Name == "pulsar_function_process_latency_ms"
	}
	fiteredMetricFamilies := filter(metricFamilies, matchFamilyFunc)
	if len(fiteredMetricFamilies) > 1 {
		t.Fatal("Too many metric families")
	}
	// Then, we need to filter the metrics in the family to one that matches our label.
	// *lbl.Name == "fqfn" && *lbl.Value == fqfn
	matchMetricFunc := func(lbl *prometheus_client.LabelPair) bool {
		return *lbl.Name == "fqfn" && *lbl.Value == "test-tenant/test-namespace/test-name"
	}
	matchingMetric := getFirstMatch(fiteredMetricFamilies[0].Metric, matchMetricFunc)
	count := matchingMetric.GetSummary().SampleCount
	sum := matchingMetric.GetSummary().SampleSum
	assert.Equal(t, 61925, int(*sum))
	assert.Equal(t, 2000, int(*count))
}

func TestMetricsServer(t *testing.T) {
	gi := newGoInstance()
	metricsServicer := NewMetricsServicer(gi)
	metricsServicer.serve()
	gi.stats.incrTotalReceived()
	time.Sleep(time.Second * 1)

	resp, err := http.Get(fmt.Sprintf("http://localhost:%d/", gi.context.GetMetricsPort()))
	assert.Equal(t, nil, err)
	assert.NotEqual(t, nil, resp)
	assert.Equal(t, 200, resp.StatusCode)
	body, err := ioutil.ReadAll(resp.Body)
	assert.Equal(t, nil, err)
	assert.NotEmpty(t, body)
	resp.Body.Close()

	resp, err = http.Get(fmt.Sprintf("http://localhost:%d/metrics", gi.context.GetMetricsPort()))
	assert.Equal(t, nil, err)
	assert.NotEqual(t, nil, resp)
	assert.Equal(t, 200, resp.StatusCode)
	body, err = ioutil.ReadAll(resp.Body)
	assert.Equal(t, nil, err)
	assert.NotEmpty(t, body)
	resp.Body.Close()
}
