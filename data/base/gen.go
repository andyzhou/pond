package base

import (
	"errors"
	"time"

	"github.com/andyzhou/tinylib/redis"
)

/*
 * @author <AndyZhou>
 * @mail <diudiu8848@163.com>
 * gen key data
 */

//face info
type GenData struct {
	cfg *redis.Config
	Base
}

//construct
func NewGenData(cfg *redis.Config) *GenData {
	//self init
	this := &GenData{
		cfg: cfg,
	}
	this.interInit()
	return this
}

//check key is exists or not
func (d *GenData) IsExists(key string) (bool, error) {
	//check
	if key == "" {
		return false, errors.New("invalid parameter")
	}

	//get connect
	conn, err := d.GetConn(d.cfg.DBTag)
	if err != nil {
		return false, err
	}
	if conn == nil {
		return false, errors.New("can't get redis connect")
	}

	//create context
	ctx, cancel := d.CreateContext()
	defer cancel()

	//add member
	isExist, subErr := conn.Exists(ctx, key).Result()
	if subErr != nil || isExist <= 0 {
		return false, subErr
	}
	return true, nil
}

//set expired time
func (d *GenData) ExpireKey(key string, expire time.Duration) error {
	//check
	if key == "" || expire < 0 {
		return errors.New("invalid parameter")
	}

	//get connect
	conn, err := d.GetConn(d.cfg.DBTag)
	if err != nil {
		return err
	}
	if conn == nil {
		return errors.New("can't get redis connect")
	}

	//create context
	ctx, cancel := d.CreateContext()
	defer cancel()

	//set key expire time
	_, err = conn.Expire(ctx, key, expire).Result()
	return err
}

//del keys
func (d *GenData) DelKey(keys ...string) error {
	//check
	if keys == nil || len(keys) <= 0 {
		return errors.New("invalid parameter")
	}

	//get connect
	conn, err := d.GetConn(d.cfg.DBTag)
	if err != nil {
		return err
	}
	if conn == nil {
		return errors.New("can't get redis connect")
	}

	//create context
	ctx, cancel := d.CreateContext()
	defer cancel()

	//del key data
	_, err = conn.Del(ctx, keys...).Result()
	return err
}

//get multi keys
func (d *GenData) GetMultiKeys(keys ...string) (map[string]interface{}, error) {
	//check
	if keys == nil || len(keys) <= 0 {
		return nil, errors.New("invalid parameter")
	}

	//get connect
	conn, err := d.GetConn(d.cfg.DBTag)
	if err != nil {
		return nil, err
	}
	if conn == nil {
		return nil, errors.New("can't get redis connect")
	}

	//create context
	ctx, cancel := d.CreateContext()
	defer cancel()

	//get multi key data
	respSlice, subErr := conn.MGet(ctx, keys...).Result()
	if subErr != nil || respSlice == nil {
		return nil, subErr
	}

	//format result
	result := make(map[string]interface{})
	for idx, key := range keys {
		val := respSlice[idx]
		result[key] = val
	}
	return result, nil
}

//get key
func (d *GenData) GetKey(key string) (string, error) {
	//check
	if key == "" {
		return "", errors.New("invalid parameter")
	}

	//get connect
	conn, err := d.GetConn(d.cfg.DBTag)
	if err != nil {
		return "", err
	}
	if conn == nil {
		return "", errors.New("can't get redis connect")
	}

	//create context
	ctx, cancel := d.CreateContext()
	defer cancel()

	//get key data
	resp, subErr := conn.Get(ctx, key).Result()
	return resp, subErr
}

//set key
func (d *GenData) SetKey(
	key string,
	val interface{},
	expire time.Duration) error {
	//check
	if key == "" || val == nil {
		return errors.New("invalid parameter")
	}

	//get connect
	conn, err := d.GetConn(d.cfg.DBTag)
	if err != nil {
		return err
	}
	if conn == nil {
		return errors.New("can't get redis connect")
	}

	//create context
	ctx, cancel := d.CreateContext()
	defer cancel()

	//save key data
	_, err = conn.Set(ctx, key, val, expire).Result()
	return err
}

////////////////
//private func
////////////////

//inter init
func (d *GenData) interInit() {
	//check or init redis conn
	d.CheckInitClient(d.cfg)
}