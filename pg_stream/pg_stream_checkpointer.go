package pg_stream

import (
	"fmt"
	"github.com/go-redis/redis/v7"
)

type PgStreamCheckPointer struct {
	redisConn *redis.Client
}

func NewPgStreamCheckPointer(addr, user, password string) (*PgStreamCheckPointer, error) {
	client := redis.NewClient(&redis.Options{
		Addr:     addr,
		Username: user,
		Password: password,
	})
	conn := client.Conn()
	result := conn.Ping()
	if result.Err() != nil {
		return nil, result.Err()
	}

	return &PgStreamCheckPointer{
		redisConn: client,
	}, nil
}

func (p *PgStreamCheckPointer) SetCheckPoint(lnsCheckPoint, replicationSlot string) error {
	return p.redisConn.Set(fmt.Sprintf("rs_checkpoint_%s", replicationSlot), lnsCheckPoint, 0).Err()
}

func (p *PgStreamCheckPointer) GetCheckPoint(replicationSlot string) string {
	result, _ := p.redisConn.Get(fmt.Sprintf("rs_checkpoint_%s", replicationSlot)).Result()
	return result
}

func (p *PgStreamCheckPointer) Close() error {
	if p.redisConn != nil {
		return p.redisConn.Close()
	}

	return nil
}
