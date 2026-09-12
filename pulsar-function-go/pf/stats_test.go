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
	"context"
	"fmt"
	"io"
	"math"
	"net/http"
	"testing"
	"time"

	"github.com/golang/protobuf/ptypes/empty"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/encoding/prototext"

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

	_, err = prototext.MarshalOptions{Indent: "  "}.Marshal(metricFamilies[0])
	assert.NoError(t, err)
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
	body, err := io.ReadAll(resp.Body)
	assert.Equal(t, nil, err)
	assert.NotEmpty(t, body)
	resp.Body.Close()

	resp, err = http.Get(fmt.Sprintf("http://localhost:%d/metrics", gi.context.GetMetricsPort()))
	assert.Equal(t, nil, err)
	assert.NotEqual(t, nil, resp)
	assert.Equal(t, 200, resp.StatusCode)
	body, err = io.ReadAll(resp.Body)
	assert.Equal(t, nil, err)
	assert.NotEmpty(t, body)
	resp.Body.Close()
	gi.close()
	metricsServicer.close()
}

// nolint
func TestUserMetrics(t *testing.T) {
	gi := newGoInstance()
	metricsServicer := NewMetricsServicer(gi)
	metricsServicer.serve()

	resp, err := http.Get(fmt.Sprintf("http://localhost:%d/metrics", gi.context.GetMetricsPort()))
	assert.Equal(t, nil, err)
	assert.NotEqual(t, nil, resp)
	assert.Equal(t, 200, resp.StatusCode)
	body, err := io.ReadAll(resp.Body)
	assert.Equal(t, nil, err)
	assert.NotEmpty(t, body)
	assert.NotContainsf(t, string(body), "pulsar_function_user_metric", "user metric should not appear yet")

	testUserMetricValues := map[string]int{"test": 1, "test2": 2}

	for labelname, value := range testUserMetricValues {
		gi.context.RecordMetric(labelname, float64(value))
	}

	time.Sleep(time.Second * 1)
	resp, err = http.Get(fmt.Sprintf("http://localhost:%d/metrics", gi.context.GetMetricsPort()))
	assert.Equal(t, nil, err)
	assert.NotEqual(t, nil, resp)
	assert.Equal(t, 200, resp.StatusCode)
	body, err = io.ReadAll(resp.Body)
	assert.Equal(t, nil, err)
	assert.NotEmpty(t, body)

	for labelname, value := range testUserMetricValues {
		for _, quantile := range []string{"0.5", "0.9", "0.99", "0.999"} {
			assert.Containsf(t, string(body), fmt.Sprintf("\n"+`pulsar_function_user_metric{cluster="pulsar-function-go",fqfn="//go-function",instance_id="pulsar-function",metric="%s",name="go-function",namespace="/",tenant="",quantile="%s"} %d`+"\n", labelname, quantile, value), "user metric %q quantile %s not found with value %d", labelname, quantile, value)
		}
	}

	resp.Body.Close()
	gi.close()
	metricsServicer.close()
}

func TestInstanceControlMetrics(t *testing.T) {
	instance := newGoInstance()
	t.Cleanup(instance.close)
	instanceClient := instanceCommunicationClient(t, instance)
	_, err := instanceClient.GetMetrics(context.Background(), &empty.Empty{})
	assert.NoError(t, err, "err communicating with instance control: %v", err)

	testLabels := []string{"userMetricControlTest1", "userMetricControlTest2"}
	for _, label := range testLabels {
		assert.NotContainsf(t, label, "user metrics should not yet contain %s", label)
	}

	for value, label := range testLabels {
		instance.context.RecordMetric(label, float64(value+1))
	}
	time.Sleep(time.Second)

	metrics, err := instanceClient.GetMetrics(context.Background(), &empty.Empty{})
	assert.NoError(t, err, "err communicating with instance control: %v", err)
	for value, label := range testLabels {
		assert.Containsf(t, metrics.UserMetrics, label, "user metrics should contain metric %s", label)
		assert.EqualValuesf(t, value+1, metrics.UserMetrics[label], "user metric %s != %d", label, value+1)
	}
}
