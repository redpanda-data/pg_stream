package pg_stream

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/lucasepe/codename"
	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/usedatabrew/pglogicalstream"
)

var randomSlotName string

var pgStreamConfigSpec = service.NewConfigSpec().
	Summary("Creates Postgres replication slot for CDC").
	Field(service.NewStringField("host").
		Description("PostgreSQL instance host").
		Example("123.0.0.1")).
	Field(service.NewIntField("port").
		Description("PostgreSQL instance port").
		Example(5432).
		Default(5432)).
	Field(service.NewStringField("user").
		Description("Username with permissions to start replication (RDS superuser)").
		Example("postgres"),
	).
	Field(service.NewStringField("password").
		Description("PostgreSQL database password")).
	Field(service.NewStringField("schema").
		Description("Schema that will be used to create replication")).
	Field(service.NewStringField("database").
		Description("PostgreSQL database name")).
	Field(service.NewStringEnumField("tls", "require", "none").
		Description("Defines whether benthos need to verify (skipinsecure) TLS configuration").
		Example("none").
		Default("none")).
	Field(service.NewBoolField("stream_snapshot").
		Description("Set `true` if you want to receive all the data that currently exist in database").
		Example(true).
		Default(false)).
	Field(service.NewFloatField("snapshot_memory_safety_factor").
		Description("Sets amout of memory that can be used to stream snapshot. If affects batch sizes. If we want to use only 25% of the memory available - put 0.25 factor. It will make initial streaming slower, but it will prevent your worker from OOM Kill").
		Example(0.2).
		Default(0.5)).
	Field(service.NewStringListField("tables").
		Example(`
			- my_table
			- my_table_2
		`).
		Description("List of tables we have to create logical replication for")).
	Field(service.NewStringField("slot_name").
		Description("PostgeSQL logical replication slot name. You can create it manually before starting the sync. If not provided will be replaced with a random one").
		Example("my_test_slot").
		Default(randomSlotName))

func newPgStreamInput(conf *service.ParsedConfig) (s service.Input, err error) {
	var (
		dbName                  string
		dbPort                  int
		dbHost                  string
		dbSchema                string
		dbUser                  string
		dbPassword              string
		dbSlotName              string
		tlsSetting              string
		tables                  []string
		streamSnapshot          bool
		snapshotMemSafetyFactor float64
	)

	dbSchema, err = conf.FieldString("schema")
	if err != nil {
		return nil, err
	}

	dbSlotName, err = conf.FieldString("slot_name")
	if err != nil {
		return nil, err
	}

	if dbSlotName == "" {
		dbSlotName = randomSlotName
	}

	dbPassword, err = conf.FieldString("password")
	if err != nil {
		return nil, err
	}

	dbUser, err = conf.FieldString("user")
	if err != nil {
		return nil, err
	}

	tlsSetting, err = conf.FieldString("tls")
	if err != nil {
		return nil, err
	}

	dbName, err = conf.FieldString("database")
	if err != nil {
		return nil, err
	}

	dbHost, err = conf.FieldString("host")
	if err != nil {
		return nil, err
	}

	dbPort, err = conf.FieldInt("port")
	if err != nil {
		return nil, err
	}

	tables, err = conf.FieldStringList("tables")
	if err != nil {
		return nil, err
	}

	streamSnapshot, err = conf.FieldBool("stream_snapshot")
	if err != nil {
		return nil, err
	}

	snapshotMemSafetyFactor, err = conf.FieldFloat("snapshot_memory_safety_factor")
	if err != nil {
		return nil, err
	}

	pgconnConfig := pgconn.Config{
		Host:     dbHost,
		Port:     uint16(dbPort),
		Database: dbName,
		User:     dbUser,
		TLSConfig: &tls.Config{
			InsecureSkipVerify: true,
		},
		Password: dbPassword,
	}

	if tlsSetting == "none" {
		pgconnConfig.TLSConfig = nil
	}

	return service.AutoRetryNacks(&pgStreamInput{
		dbConfig:                pgconnConfig,
		streamSnapshot:          streamSnapshot,
		snapshotMemSafetyFactor: snapshotMemSafetyFactor,
		slotName:                dbSlotName,
		schema:                  dbSchema,
		tls:                     pglogicalstream.TlsVerify(tlsSetting),
		tables:                  tables,
	}), err
}

func init() {
	rng, _ := codename.DefaultRNG()
	randomSlotName = fmt.Sprintf("%s", strings.ReplaceAll(codename.Generate(rng, 5), "-", "_"))

	err := service.RegisterInput(
		"pg_stream", pgStreamConfigSpec,
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Input, error) {
			return newPgStreamInput(conf)
		})
	if err != nil {
		panic(err)
	}
}

type pgStreamInput struct {
	dbConfig                pgconn.Config
	pglogicalStream         *pglogicalstream.Stream
	redisUri                string
	slotName                string
	schema                  string
	tables                  []string
	streamSnapshot          bool
	tls                     pglogicalstream.TlsVerify // none, require
	snapshotMemSafetyFactor float64
	logger                  *service.Logger
}

func (p *pgStreamInput) Connect(ctx context.Context) error {
	pgStream, err := pglogicalstream.NewPgStream(pglogicalstream.Config{
		DbHost:                     p.dbConfig.Host,
		DbPassword:                 p.dbConfig.Password,
		DbUser:                     p.dbConfig.User,
		DbPort:                     int(p.dbConfig.Port),
		DbTables:                   p.tables,
		DbName:                     p.dbConfig.Database,
		DbSchema:                   p.schema,
		ReplicationSlotName:        fmt.Sprintf("rs_%s", p.slotName),
		TlsVerify:                  p.tls,
		StreamOldData:              p.streamSnapshot,
		SnapshotMemorySafetyFactor: p.snapshotMemSafetyFactor,
		SeparateChanges:            true,
	})
	if err != nil {
		panic(err)
	}
	p.pglogicalStream = pgStream
	return err
}

func (p *pgStreamInput) Read(ctx context.Context) (*service.Message, service.AckFunc, error) {
	select {
	case snapshotMessage := <-p.pglogicalStream.SnapshotMessageC():
		var (
			mb  []byte
			err error
		)
		if mb, err = json.Marshal(snapshotMessage); err != nil {
			return nil, nil, err
		}
		return service.NewMessage(mb), func(ctx context.Context, err error) error {
			// Nacks are retried automatically when we use service.AutoRetryNacks
			return nil
		}, nil
	case message := <-p.pglogicalStream.LrMessageC():
		var (
			mb  []byte
			err error
		)
		if mb, err = json.Marshal(message); err != nil {
			return nil, nil, err
		}
		return service.NewMessage(mb), func(ctx context.Context, err error) error {
			// Nacks are retried automatically when we use service.AutoRetryNacks
			//message.ServerHeartbeat.

			if message.Lsn != nil {
				p.pglogicalStream.AckLSN(*message.Lsn)
			}
			return nil
		}, nil
	case <-ctx.Done():
		return nil, nil, p.pglogicalStream.Stop()
	}
}

func (p *pgStreamInput) Close(ctx context.Context) error {
	if p.pglogicalStream != nil {
		return p.pglogicalStream.Stop()
	}
	return nil
}
