package base

import (
	"errors"
	"fmt"
	"github.com/andyzhou/pond/define"

	"github.com/andyzhou/tinylib/redis"
	genRedis "github.com/go-redis/redis/v8"
)

/*
 * @author <AndyZhou>
 * @mail <diudiu8848@163.com>
 * general hash data
 */

//data face
type HashData struct {
	cfg *redis.Config
	Base
}

//construct
func NewHashData(cfg *redis.Config) *HashData {
	//self init
	this := &HashData{
		cfg: cfg,
	}
	this.interInit()
	return this
}

//delete one tag
func (d *HashData) DelOne(tag string) error {
	//check
	if tag == "" {
		return errors.New("invalid parameter")
	}

	//get key and connect
	connect, key, err := d.getKeyConnect(tag)
	if err != nil {
		return err
	}

	//create context
	ctx, cancel := d.CreateContext()
	defer cancel()

	//del one key
	_, err = connect.Del(ctx, key).Result()
	return err
}

//delete fields
func (d *HashData) DelFields(
	tag string,
	fields ...string) error {
	//check
	if tag == "" || fields == nil || len(fields) <= 0 {
		return errors.New("invalid parameter")
	}

	//get key and connect
	connect, key, err := d.getKeyConnect(tag)
	if err != nil {
		return err
	}

	//create context
	ctx, cancel := d.CreateContext()
	defer cancel()

	//del batch fields
	_, err = connect.HDel(ctx, key, fields...).Result()
	return err
}

//get one field value
func (d *HashData) GetOneValue(
		tag string,
		field string,
	) (string, error) {
	//check
	if tag == "" || field == "" {
		return "", errors.New("invalid parameter")
	}

	//get key and connect
	connect, key, err := d.getKeyConnect(tag)
	if err != nil {
		return "", err
	}

	//create context
	ctx, cancel := d.CreateContext()
	defer cancel()

	//get one field value
	value, err := connect.HGet(ctx, key, field).Result()
	if err != nil && err.Error() == redis.Nil {
		return "", nil
	}
	return value, err
}

//get batch fields value
func (d *HashData) GetValues(
		tag string,
		fields ...string,
	) (map[string]interface{}, error) {
	//check
	if tag == "" || fields == nil || len(fields) <= 0 {
		return nil, errors.New("invalid parameter")
	}

	//get key and connect
	connect, key, err := d.getKeyConnect(tag)
	if err != nil {
		return nil, err
	}

	//create context
	ctx, cancel := d.CreateContext()
	defer cancel()

	//get fields value
	recSlice, subErr := connect.HMGet(ctx, key, fields...).Result()
	if subErr != nil || recSlice == nil {
		return nil, subErr
	}

	//format result
	result := map[string]interface{}{}
	for idx, value := range recSlice {
		//check
		if value == nil || value == "" {
			continue
		}
		//get field info
		field := fields[idx]

		//fill result
		result[field] = value
	}
	return result, nil
}

//get all fields value
func (d *HashData) GetAllFields(
		tag string,
	) (map[string]string, error) {
	//check
	if tag == "" {
		return nil, errors.New("invalid parameter")
	}

	//get key and connect
	connect, key, err := d.getKeyConnect(tag)
	if err != nil {
		return nil, err
	}

	//create context
	ctx, cancel := d.CreateContext()
	defer cancel()

	//get all fields value
	recSlice, subErr := connect.HGetAll(ctx, key).Result()
	return recSlice, subErr
}

//set one field and value
func (d *HashData) SetOneValue(
	tag,
	field string,
	value interface{}) error {
	//check
	if tag == "" || field == "" || value == nil {
		return errors.New("invalid parameter")
	}

	//get key and connect
	connect, key, err := d.getKeyConnect(tag)
	if err != nil {
		return err
	}

	//create context
	ctx, cancel := d.CreateContext()
	defer cancel()

	//set one field value
	_, err = connect.HSet(ctx, key, field, value).Result()
	return err
}

//set batch field and values
//fieldMap, field -> value
func (d *HashData) SetValues(
	tag string,
	fieldMap map[string]interface{}) error {
	//check
	if tag == "" || fieldMap == nil || len(fieldMap) <= 0 {
		return errors.New("invalid parameter")
	}

	//get key and connect
	connect, key, err := d.getKeyConnect(tag)
	if err != nil {
		return err
	}

	//format field and values
	values := make([]interface{}, 0)
	for k, v := range fieldMap {
		values = append(values, k, v)
	}

	//create context
	ctx, cancel := d.CreateContext()
	defer cancel()

	//set fields value
	_, err = connect.HMSet(ctx, key, values...).Result()
	return err
}

//get key
func (d *HashData) GetKey(tag string) string {
	return d.getKey(tag)
}

////////////////
//private func
////////////////

//get key, connect obj
func (d *HashData) getKeyConnect(
		tag string,
	) (*genRedis.Conn, string, error) {
	//get key
	key := d.getKey(tag)

	//get connect
	connect, err := d.GetConn(d.cfg.DBTag)
	if err != nil {
		return nil, key, err
	}
	if connect == nil {
		return nil, key, errors.New("can't get redis connect")
	}
	return connect, key, nil
}

//get set key
func (d *HashData) getKey(tag string) string {
	return fmt.Sprintf(define.RedisKeyHashPattern, d.cfg.DBTag, tag)
}

//inter init
func (d *HashData) interInit() {
	//check or init redis conn
	d.CheckInitClient(d.cfg)
}