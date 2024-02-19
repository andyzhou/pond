package data

import (
	"github.com/andyzhou/pond/conf"
	"sync"
)

/*
 * inter redis data face
 */

//global variable
var (
	_redis *InterRedisData
	_redisOnce sync.Once
)

//data face
type InterRedisData struct {
	file *FileData
}

//get single instance
func GetRedisData() *InterRedisData {
	_redisOnce.Do(func() {
		_redis = NewInterRedisData()
	})
	return _redis
}

//construct
func NewInterRedisData() *InterRedisData {
	this := &InterRedisData{
		file: NewFileData(),
	}
	return this
}

//set redis config, must call!!!
func (f *InterRedisData) SetRedisConf(cfg *conf.RedisConfig) {
	f.file.SetRedisConf(cfg)
}

//get relate data face
func (f *InterRedisData) GetFile() *FileData {
	return f.file
}