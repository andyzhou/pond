package data

import (
	"github.com/andyzhou/pond/conf"
)

/*
 * @author <AndyZhou>
 * @mail <diudiu8848@163.com>
 * inter redis data face
 */

//data face
type InterRedisData struct {
	file *FileData
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