// Package cdc streams queued-notification changes from Postgres logical
// replication into the priority Kafka topics that drive delivery.
package cdc

import (
	"context"
	"fmt"

	pqcdc "github.com/Trendyol/go-pq-cdc-kafka"
	pqkafkacfg "github.com/Trendyol/go-pq-cdc-kafka/config"
	basecfg "github.com/Trendyol/go-pq-cdc/config"
	"github.com/Trendyol/go-pq-cdc/pq/publication"
	"github.com/Trendyol/go-pq-cdc/pq/slot"
	"github.com/jackc/pgx/v5/pgconn"
)

// Config holds the inputs needed to start the CDC connector: the Postgres DSN,
// replication slot and publication names, and Kafka brokers.
type Config struct {
	DSN             string
	SlotName        string
	PublicationName string
	Brokers         []string
}

// Run starts the CDC connector and blocks until ctx is canceled. The row-filtered
// publication (status='queued') captures only the scheduled->queued transition,
// so later status write-backs never replicate.
func Run(ctx context.Context, cfg Config, handler pqcdc.Handler) error {
	connCfg, err := buildConnectorConfig(cfg)
	if err != nil {
		return err
	}
	connector, err := pqcdc.NewConnector(ctx, connCfg, handler)
	if err != nil {
		return fmt.Errorf("new cdc connector: %w", err)
	}
	defer connector.Close()
	connector.Start(ctx)
	return nil
}

func buildConnectorConfig(cfg Config) (pqkafkacfg.Connector, error) {
	pc, err := pgconn.ParseConfig(cfg.DSN)
	if err != nil {
		return pqkafkacfg.Connector{}, fmt.Errorf("parse dsn: %w", err)
	}
	tables := publication.Tables{
		{Name: tableNotifications, Schema: schemaPublic, ReplicaIdentity: publication.ReplicaIdentityFull},
	}
	return pqkafkacfg.Connector{
		CDC: basecfg.Config{
			Host:     pc.Host,
			Port:     int(pc.Port),
			Username: pc.User,
			Password: pc.Password,
			Database: pc.Database,
			Slot: slot.Config{
				Name:              cfg.SlotName,
				CreateIfNotExists: true,
			},
			Publication: publication.Config{
				Name:              cfg.PublicationName,
				Operations:        publication.Operations{publication.OperationInsert, publication.OperationUpdate},
				Tables:            tables,
				CreateIfNotExists: true,
			},
		},
		Kafka: pqkafkacfg.Kafka{
			Brokers:                cfg.Brokers,
			TableTopicMapping:      map[string]string{},
			AllowAutoTopicCreation: true,
		},
	}, nil
}
