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
	"math"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/prometheus/client_golang/prometheus"
	io_prometheus_client "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
)

/*func test(){
	var metrics_label_names = []string{"tenant", "namespace", "name", "instance_id", "cluster", "fqfn"}
	var exception_label_names = []string{"error", "ts"}
	var exception_metrics_label_names = append(metrics_label_names, exception_label_names...)
	var stat_process_latency_ms = prometheus.NewSummaryVec(
		prometheus.SummaryOpts{
			Name: PULSAR_FUNCTION_METRICS_PREFIX + PROCESS_LATENCY_MS,
			Help: "Process latency in milliseconds."}, metrics_label_names)
	var reg *prometheus.Registry
	reg = prometheus.NewRegistry()
	reg.MustRegister(stat_process_latency_ms)
	metrics_labels := []string{"test-tenant","test-tenant/test-namespace", "test-name", "1234", "test-cluster",
		"test-tenant/test-namespace/test-name"}
	// 1234 is instanceId
	// ['test-tenant', 'test-tenant/test-namespace', 'test-name',1234,
    //    'test-cluster', 'test-tenant/test-namespace/test-name']
	//var _stat_process_latency_ms = stat_process_latency_ms.WithLabelValues(metrics_labels...)
	//process_latency_ms_count := stat._stat_process_latency_ms._count.get()
	//process_latency_ms_sum := stat._stat_process_latency_ms._sum.get()
}
func  (stat *StatWithLabelValues) getTotalReceived() float32 {
	gathering, _ := reg.Gather()
	out := &bytes.Buffer{}
	for _, mf := range gathering {
		if _, err := expfmt.MetricFamilyToText(out, mf); err != nil {
			panic(err)
		}
	}
	fmt.Print(out.String())
	fmt.Println("----------")
}
*/
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
		panic("unexpected behavior of custom test registry")
	}
	match := func(vect *io_prometheus_client.MetricFamily) bool {
		return *vect.Name == "pond_temperature_celsius"
	}
	fiteredMetricFamilies := filter(metricFamilies, match)

	if len(fiteredMetricFamilies) > 1 {
		panic("Too many metric families")
	}
	// Then, we need to filter the metrics in the family to one that matches our label.

	fmt.Println(proto.MarshalTextString(metricFamilies[0]))

	// Output:
	// name: "pond_temperature_celsius"
	// help: "The temperature of the frog pond."
	// type: SUMMARY
	// metric: <
	//   label: <
	//     name: "species"
	//     value: "leiopelma-hochstetteri"
	//   >
	//   summary: <
	//     sample_count: 0
	//     sample_sum: 0
	//     quantile: <
	//       quantile: 0.5
	//       value: nan
	//     >
	//     quantile: <
	//       quantile: 0.9
	//       value: nan
	//     >
	//     quantile: <
	//       quantile: 0.99
	//       value: nan
	//     >
	//   >
	// >
	// metric: <
	//   label: <
	//     name: "species"
	//     value: "lithobates-catesbeianus"
	//   >
	//   summary: <
	//     sample_count: 1000
	//     sample_sum: 31956.100000000017
	//     quantile: <
	//       quantile: 0.5
	//       value: 32.4
	//     >
	//     quantile: <
	//       quantile: 0.9
	//       value: 41.4
	//     >
	//     quantile: <
	//       quantile: 0.99
	//       value: 41.9
	//     >
	//   >
	// >
	// metric: <
	//   label: <
	//     name: "species"
	//     value: "litoria-caerulea"
	//   >
	//   summary: <
	//     sample_count: 1000
	//     sample_sum: 29969.50000000001
	//     quantile: <
	//       quantile: 0.5
	//       value: 31.1
	//     >
	//     quantile: <
	//       quantile: 0.9
	//       value: 41.3
	//     >
	//     quantile: <
	//       quantile: 0.99
	//       value: 41.9
	//     >
	//   >
	// >
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
		panic("unexpected behavior of custom test registry")
	}
	matchFamilyFunc := func(vect *io_prometheus_client.MetricFamily) bool {
		return *vect.Name == "pulsar_function_process_latency_ms"
	}
	fiteredMetricFamilies := filter(metricFamilies, matchFamilyFunc)
	if len(fiteredMetricFamilies) > 1 {
		panic("Too many metric families")
	}
	// Then, we need to filter the metrics in the family to one that matches our label.
	// *lbl.Name == "fqfn" && *lbl.Value == fqfn
	matchMetricFunc := func(lbl *io_prometheus_client.LabelPair) bool {
		return *lbl.Name == "fqfn" && *lbl.Value == "test-tenant/test-namespace/test-name"
	}
	matchingMetric := getFirstMatch(fiteredMetricFamilies[0].Metric, matchMetricFunc)
	count := matchingMetric.GetSummary().SampleCount
	sum := matchingMetric.GetSummary().SampleSum
	assert.Equal(t, 61925, int(*sum))
	assert.Equal(t, 2000, int(*count))
}
