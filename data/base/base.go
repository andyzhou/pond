package base

import (
	"context"
	"errors"
	"time"

	"github.com/andyzhou/pond/conf"
	"github.com/andyzhou/pond/define"
	"github.com/andyzhou/tinylib/redis"
	genRedis "github.com/go-redis/redis/v8"
)

/*
 * @author <AndyZhou>
 * @mail <diudiu8848@163.com>
 * base redis data face
 */

//inter macro define
const (
	DefaultContextTimeOut = time.Second * 30
)

//data face
type Base struct {
	r *redis.Redis
}

//create context
func (f *Base) CreateContext(
		durations ...time.Duration,
	) (context.Context, context.CancelFunc) {
	var (
		duration time.Duration
	)
	if durations != nil && len(durations) <= 0 {
		duration = durations[0]
	}
	if duration <= 0 {
		duration = DefaultContextTimeOut
	}
	return context.WithTimeout(context.Background(), duration)
}

//get redis conn
func (f *Base) GetConn(dbTag string) (*genRedis.Conn, error) {
	//check
	if dbTag == "" {
		return nil, errors.New("invalid parameter")
	}
	if f.r == nil {
		return nil, errors.New("redis hasn't init")
	}

	//get target connect by db tag
	conn := f.r.C(dbTag)
	if conn == nil {
		return nil, errors.New("no connection for db tag")
	}
	return conn.GetConnect(), nil
}

//check or init redis client
func (f *Base) CheckInitClient(cfg *redis.Config) (*redis.Connection, error) {
	//check
	if cfg == nil || cfg.DBTag == "" {
		return nil, errors.New("invalid parameter")
	}

	//init and connect redis
	if f.r == nil {
		f.r = redis.NewRedis()
	}
	conn, err := f.r.CreateConn(cfg)
	return conn, err
}

//gen redis config
func (f *Base) GenRedisConf(cfg *conf.RedisConfig) *redis.Config {
	redisCfg := &redis.Config{
		DBTag: cfg.GroupTag,
		Addr: cfg.Address,
		Password: cfg.Password,
		DBNum: cfg.DBNum,
		TimeOut: time.Duration(define.DefaultConnTimeOut) * time.Second,
		PoolSize: cfg.Pools,
	}
	return redisCfg
}
