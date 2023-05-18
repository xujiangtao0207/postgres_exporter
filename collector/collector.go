// Copyright 2022 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package collector

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"github.com/lib/pq"
	"strings"
	"sync"
	"time"

	"github.com/alecthomas/kingpin/v2"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	factories              = make(map[string]func(collectorConfig) (Collector, error))
	initiatedCollectorsMtx = sync.Mutex{}
	initiatedCollectors    = make(map[string]Collector)
	collectorState         = make(map[string]*bool)
	forcedCollectors       = map[string]bool{} // collectors which have been explicitly enabled or disabled
)

const (
	// Namespace for all metrics.
	namespace = "pg"

	defaultEnabled = true
	// defaultDisabled = false
)

var (
	scrapeDurationDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "scrape", "collector_duration_seconds"),
		"postgres_exporter: Duration of a collector scrape.",
		[]string{"collector", serverLabelName},
		nil,
	)
	scrapeSuccessDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "scrape", "collector_success"),
		"postgres_exporter: Whether a collector succeeded.",
		[]string{"collector", serverLabelName},
		nil,
	)
)

type Collector interface {
	Update(ctx context.Context, labels prometheus.Labels, db *sql.DB, ch chan<- prometheus.Metric) error
}

type collectorConfig struct {
	logger           log.Logger
	excludeDatabases []string
}

func registerCollector(name string, isDefaultEnabled bool, createFunc func(collectorConfig) (Collector, error)) {
	var helpDefaultState string
	if isDefaultEnabled {
		helpDefaultState = "enabled"
	} else {
		helpDefaultState = "disabled"
	}

	// Create flag for this collector
	flagName := fmt.Sprintf("collector.%s", name)
	flagHelp := fmt.Sprintf("Enable the %s collector (default: %s).", name, helpDefaultState)
	defaultValue := fmt.Sprintf("%v", isDefaultEnabled)

	flag := kingpin.Flag(flagName, flagHelp).Default(defaultValue).Action(collectorFlagAction(name)).Bool()
	collectorState[name] = flag

	// Register the create function for this collector
	factories[name] = createFunc
}

// PostgresCollector implements the prometheus.Collector interface.
type PostgresCollector struct {
	Collectors map[string]Collector
	logger     log.Logger

	dbs []*sql.DB

	labels []prometheus.Labels
}

type Option func(*PostgresCollector) error

// NewPostgresCollector creates a new PostgresCollector.
func NewPostgresCollector(logger log.Logger, excludeDatabases []string, dsns []string, filters []string, options ...Option) (*PostgresCollector, error) {
	p := &PostgresCollector{
		logger: logger,
	}
	// Apply options to customize the collector
	for _, o := range options {
		err := o(p)
		if err != nil {
			return nil, err
		}
	}

	f := make(map[string]bool)
	for _, filter := range filters {
		enabled, exist := collectorState[filter]
		if !exist {
			return nil, fmt.Errorf("missing collector: %s", filter)
		}
		if !*enabled {
			return nil, fmt.Errorf("disabled collector: %s", filter)
		}
		f[filter] = true
	}
	collectors := make(map[string]Collector)
	initiatedCollectorsMtx.Lock()
	defer initiatedCollectorsMtx.Unlock()
	for key, enabled := range collectorState {
		if !*enabled || (len(f) > 0 && !f[key]) {
			continue
		}
		if collector, ok := initiatedCollectors[key]; ok {
			collectors[key] = collector
		} else {
			collector, err := factories[key](collectorConfig{
				logger:           log.With(logger, "collector", key),
				excludeDatabases: excludeDatabases,
			})
			if err != nil {
				return nil, err
			}
			collectors[key] = collector
			initiatedCollectors[key] = collector
		}
	}

	p.Collectors = collectors

	if len(dsns) == 0 {
		return nil, errors.New("empty dsn")
	}

	for _, dsn := range dsns {
		db, err := sql.Open("postgres", dsn)
		if err != nil {
			return nil, err
		}
		db.SetMaxOpenConns(1)
		db.SetMaxIdleConns(1)

		p.dbs = append(p.dbs, db)

		fingerprint, err := parseFingerprint(dsn)
		if err != nil {
			return nil, err
		}

		p.labels = append(p.labels, prometheus.Labels{
			serverLabelName: fingerprint,
		})
	}

	return p, nil
}

// Describe implements the prometheus.Collector interface.
func (p PostgresCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- scrapeDurationDesc
	ch <- scrapeSuccessDesc
}

// Collect implements the prometheus.Collector interface.
func (p PostgresCollector) Collect(ch chan<- prometheus.Metric) {
	ctx := context.TODO()
	wg := sync.WaitGroup{}
	wg.Add(len(p.Collectors))
	for name, c := range p.Collectors {
		go func(name string, c Collector) {
			for i := 0; i < len(p.dbs); i++ {
				execute(ctx, name, c, p.labels[i], p.dbs[i], ch, p.logger)
			}
			wg.Done()
		}(name, c)
	}
	wg.Wait()
}

func execute(ctx context.Context, name string, c Collector, labels prometheus.Labels, db *sql.DB, ch chan<- prometheus.Metric, logger log.Logger) {
	begin := time.Now()
	err := c.Update(ctx, labels, db, ch)
	duration := time.Since(begin)
	var success float64

	if err != nil {
		if IsNoDataError(err) {
			level.Debug(logger).Log("msg", "collector returned no data", "name", name, "duration_seconds", duration.Seconds(), "err", err)
		} else {
			level.Error(logger).Log("msg", "collector failed", "name", name, "duration_seconds", duration.Seconds(), "err", err)
		}
		success = 0
	} else {
		level.Debug(logger).Log("msg", "collector succeeded", "name", name, "duration_seconds", duration.Seconds())
		success = 1
	}
	ch <- prometheus.MustNewConstMetric(scrapeDurationDesc, prometheus.GaugeValue, duration.Seconds(), name, labels[serverLabelName])
	ch <- prometheus.MustNewConstMetric(scrapeSuccessDesc, prometheus.GaugeValue, success, name, labels[serverLabelName])
}

// collectorFlagAction generates a new action function for the given collector
// to track whether it has been explicitly enabled or disabled from the command line.
// A new action function is needed for each collector flag because the ParseContext
// does not contain information about which flag called the action.
// See: https://github.com/alecthomas/kingpin/issues/294
func collectorFlagAction(collector string) func(ctx *kingpin.ParseContext) error {
	return func(ctx *kingpin.ParseContext) error {
		forcedCollectors[collector] = true
		return nil
	}
}

// ErrNoData indicates the collector found no data to collect, but had no other error.
var ErrNoData = errors.New("collector returned no data")

func IsNoDataError(err error) bool {
	return err == ErrNoData
}

func parseFingerprint(url string) (string, error) {
	dsn, err := pq.ParseURL(url)
	if err != nil {
		dsn = url
	}

	pairs := strings.Split(dsn, " ")
	kv := make(map[string]string, len(pairs))
	for _, pair := range pairs {
		splitted := strings.SplitN(pair, "=", 2)
		if len(splitted) != 2 {
			return "", fmt.Errorf("malformed dsn %q", dsn)
		}
		// Newer versions of pq.ParseURL quote values so trim them off if they exist
		key := strings.Trim(splitted[0], "'\"")
		value := strings.Trim(splitted[1], "'\"")
		kv[key] = value
	}

	var fingerprint string

	if host, ok := kv["host"]; ok {
		fingerprint += host
	} else {
		fingerprint += "localhost"
	}

	if port, ok := kv["port"]; ok {
		fingerprint += ":" + port
	} else {
		fingerprint += ":5432"
	}

	return fingerprint, nil
}
