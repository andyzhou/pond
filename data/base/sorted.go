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
 * general sorted data
 * - use sorted set as storage mode
 */

//face info
type SortedData struct {
	cfg *redis.Config
	Base
}

//construct
func NewSortedData(cfg *redis.Config) *SortedData {
	//self init
	this := &SortedData{
		cfg: cfg,
	}
	this.interInit()
	return this
}

//clear
func (d *SortedData) Clear(
	tag string) error {
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

	//clear key
	_, err = connect.Del(ctx, key).Result()
	return err
}

//get total count
func (d *SortedData) GetTotalCount(
	tag string) (int64, error) {
	//check
	if tag == "" {
		return 0, errors.New("invalid parameter")
	}

	//get key and connect
	connect, key, err := d.getKeyConnect(tag)
	if err != nil {
		return 0, err
	}

	//create context
	ctx, cancel := d.CreateContext()
	defer cancel()

	//get total count
	total, subErr := connect.ZCard(ctx, key).Result()
	return total, subErr
}

//get greater member by score
func (d *SortedData) GetGreaterMemberByScore(
		tag string,
		score float64,
	) (*genRedis.Z, error) {
	//check
	if tag == "" || score < 0 {
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

	//set key
	keys := []string{
		key,
	}

	//set args
	args := make([]interface{}, 0)
	args = append(args, score)

	//load lua script
	script := LuaScriptOfPickSortedNearMember
	scriptSha, subErr := connect.ScriptLoad(ctx, script).Result()
	if subErr != nil {
		return nil, subErr
	}

	//run lua script
	resp, subErrTwo := connect.EvalSha(
		ctx,
		scriptSha,
		keys,
		args...,
	).Result()
	if subErrTwo != nil || resp == nil {
		return nil, subErrTwo
	}
	return nil, nil
}

//get batch members
//sorted by score
func (d *SortedData) GetBatchMembers(
		tag string,
		start, end int,
		isByDesc ...bool,
	) ([]genRedis.Z, error) {
	var (
		isZRevRange bool
		zSlice []genRedis.Z
		err error
	)
	//check
	if tag == "" {
		return nil, errors.New("invalid parameter")
	}
	if start < 0 {
		start = 0
	}
	if end <= 0 {
		end = define.RecPerPage
	}
	if isByDesc != nil && len(isByDesc) > 0 {
		isZRevRange = isByDesc[0]
	}

	//get key and connect
	connect, key, subErr := d.getKeyConnect(tag)
	if subErr != nil {
		return nil, subErr
	}

	//create context
	ctx, cancel := d.CreateContext()
	defer cancel()

	//get batch data with score value
	if isZRevRange {
		//desc order
		zSlice, err = connect.ZRevRangeWithScores(ctx, key, int64(start), int64(end)).Result()
	}else{
		//asc order
		zSlice, err = connect.ZRevRangeWithScores(ctx, key, int64(start), int64(end)).Result()
	}
	if err != nil {
		return nil, err
	}
	if zSlice == nil || len(zSlice) <= 0 {
		return nil, nil
	}
	return zSlice, nil
}

//remove member
func (d *SortedData) RemoveMember(
	tag string,
	members ...interface{}) error {
	//check
	if tag == "" || members == nil || len(members) <= 0 {
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

	//try remove relate members
	_, err = connect.ZRem(ctx, key, members...).Result()
	return err
}

//incr/decr member score
func (d *SortedData) IncByScore(
	tag, member string,
	incVal float64) error {
	//check
	if tag == "" || member == "" || incVal == 0 {
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

	//inc count value
	_, err = connect.ZIncrBy(ctx, key, incVal, member).Result()
	return err
}

//add batch members
func (d *SortedData) AddMembers(
	tag string,
	members ...*genRedis.Z) error {
	//check
	if tag == "" || members == nil || len(members) <= 0 {
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

	//add members
	_, err = connect.ZAdd(ctx, key, members...).Result()
	return err
}

//gen new member
func (d *SortedData) GenMember(member interface{}, score float64) *genRedis.Z {
	return &genRedis.Z{
		Member: member,
		Score: score,
	}
}

//get key
func (d *SortedData) GetKey(tag string) string {
	return d.getKey(tag)
}

////////////////
//private func
////////////////

//get key, connect obj
func (d *SortedData) getKeyConnect(
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

//get sorted key
func (d *SortedData) getKey(tag string) string {
	return fmt.Sprintf(define.RedisKeySortedPattern, d.cfg.DBTag, tag)
}

//inter init
func (d *SortedData) interInit() {
	//check or init redis conn
	d.CheckInitClient(d.cfg)
}