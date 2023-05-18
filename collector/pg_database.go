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

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
)

func init() {
	registerCollector("database", defaultEnabled, NewPGDatabaseCollector)
}

type PGDatabaseCollector struct {
	log               log.Logger
	excludedDatabases []string
}

const (
	serverLabelName = "server"
)

func NewPGDatabaseCollector(config collectorConfig) (Collector, error) {
	exclude := config.excludeDatabases
	if exclude == nil {
		exclude = []string{}
	}
	return &PGDatabaseCollector{
		log:               config.logger,
		excludedDatabases: exclude,
	}, nil
}

var pgDatabase = map[string]*prometheus.Desc{
	"size_bytes": prometheus.NewDesc(
		"pg_database_size_bytes",
		"Disk space used by the database",
		[]string{"datname", serverLabelName}, nil,
	),
	"xid_ages": prometheus.NewDesc(
		"pg_database_xid_ages",
		"Age by the database",
		[]string{"datname", serverLabelName}, nil,
	),
}

type PGDatabase struct {
	DatName string
	Size    int64
	Age     int64
}

// Update implements Collector and exposes database size.
// It is called by the Prometheus registry when collecting metrics.
// The list of databases is retrieved from pg_database and filtered
// by the excludeDatabase config parameter. The tradeoff here is that
// we have to query the list of databases and then query the size of
// each database individually. This is because we can't filter the
// list of databases in the query because the list of excluded
// databases is dynamic.
func (c PGDatabaseCollector) Update(ctx context.Context, labels prometheus.Labels, db *sql.DB, ch chan<- prometheus.Metric) error {
	// Query the list of databases
	rows, err := db.QueryContext(ctx,
		`SELECT pg_database.datname
		,pg_database_size(pg_database.datname)
        ,age(datfrozenxid)
		FROM pg_database;`,
	)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {

		var database PGDatabase
		if err := rows.Scan(&database.DatName, &database.Size, &database.Age); err != nil {
			return err
		}

		ch <- prometheus.MustNewConstMetric(
			pgDatabase["size_bytes"],
			prometheus.GaugeValue, float64(database.Size), database.DatName, labels[serverLabelName],
		)

		ch <- prometheus.MustNewConstMetric(
			pgDatabase["xid_ages"],
			prometheus.GaugeValue, float64(database.Age), database.DatName, labels[serverLabelName],
		)

	}

	if err := rows.Err(); err != nil {
		return err
	}
	return nil
}

func sliceContains(slice []string, s string) bool {
	for _, item := range slice {
		if item == s {
			return true
		}
	}
	return false
}
