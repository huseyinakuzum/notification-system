package cdc

import (
	"testing"

	"github.com/Trendyol/go-pq-cdc/pq/publication"
)

func TestBuildConnectorConfig(t *testing.T) {
	cfg, err := buildConnectorConfig(Config{
		DSN:             "postgres://nsys:nsys@db-host:5432/nsys?sslmode=disable",
		SlotName:        "nsys",
		PublicationName: "nsys_pub",
		Brokers:         []string{"kafka:9092"},
	})
	if err != nil {
		t.Fatalf("build: %v", err)
	}

	cdc := cfg.CDC
	if cdc.Host != "db-host" {
		t.Errorf("host = %q, want db-host", cdc.Host)
	}
	if cdc.Port != 5432 {
		t.Errorf("port = %d, want 5432", cdc.Port)
	}
	if cdc.Username != "nsys" || cdc.Password != "nsys" {
		t.Errorf("creds = %q/%q, want nsys/nsys", cdc.Username, cdc.Password)
	}
	if cdc.Database != "nsys" {
		t.Errorf("database = %q, want nsys", cdc.Database)
	}
	if cdc.Slot.Name != "nsys" || !cdc.Slot.CreateIfNotExists {
		t.Errorf("slot = %+v, want name nsys createIfNotExists", cdc.Slot)
	}
	if cdc.Publication.Name != "nsys_pub" || !cdc.Publication.CreateIfNotExists {
		t.Errorf("publication = %+v, want name nsys_pub createIfNotExists", cdc.Publication)
	}
	wantOps := map[publication.Operation]bool{
		publication.OperationInsert: true,
		publication.OperationUpdate: true,
	}
	if len(cdc.Publication.Operations) != 2 {
		t.Errorf("operations = %v, want [INSERT UPDATE]", cdc.Publication.Operations)
	}
	for _, op := range cdc.Publication.Operations {
		if !wantOps[op] {
			t.Errorf("unexpected operation %v, want INSERT/UPDATE", op)
		}
	}

	gotTables := map[string]bool{}
	for _, tb := range cdc.Publication.Tables {
		if tb.Schema != "public" {
			t.Errorf("table %s schema = %q, want public", tb.Name, tb.Schema)
		}
		if tb.ReplicaIdentity != publication.ReplicaIdentityFull {
			t.Errorf("table %s replica identity = %q, want FULL", tb.Name, tb.ReplicaIdentity)
		}
		gotTables[tb.Name] = true
	}
	if len(gotTables) != 1 || !gotTables["notifications"] {
		t.Errorf("tables = %v, want notifications only", gotTables)
	}

	if len(cfg.Kafka.Brokers) != 1 || cfg.Kafka.Brokers[0] != "kafka:9092" {
		t.Errorf("brokers = %v, want [kafka:9092]", cfg.Kafka.Brokers)
	}
	if !cfg.Kafka.AllowAutoTopicCreation {
		t.Error("AllowAutoTopicCreation = false, want true")
	}
	if len(cfg.Kafka.TableTopicMapping) != 0 {
		t.Errorf("TableTopicMapping = %v, want empty (per-message routing)", cfg.Kafka.TableTopicMapping)
	}
}

func TestBuildConnectorConfigBadDSN(t *testing.T) {
	if _, err := buildConnectorConfig(Config{DSN: "://nope"}); err == nil {
		t.Fatal("want error for bad dsn, got nil")
	}
}
