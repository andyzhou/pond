package pond

import (
	"errors"
	"github.com/andyzhou/pond/conf"
	"github.com/andyzhou/pond/define"
	"github.com/andyzhou/pond/json"
	"github.com/andyzhou/pond/storage"
	"sync"
)

/*
 * api interface
 * @author <AndyZhou>
 * @mail <diudiu8848@163.com>
 * - service one single node or group data
 */

//global variable
var (
	_pond *Pond
	_pondOnce sync.Once
	_wg sync.WaitGroup
)

//face info
type Pond struct {
	storage *storage.Storage
}

//get single instance
func GetPond() *Pond {
	_pondOnce.Do(func() {
		_pond = NewPond()
	})
	return _pond
}

//construct
func NewPond() *Pond {
	this := &Pond{
		storage: storage.NewStorage(&_wg),
	}
	return this
}

//quit
func (f *Pond) Quit() {
	f.storage.Quit()
}

//wait
func (f *Pond) Wait() {
	_wg.Wait()
}

////////////////////
//api for file index
////////////////////

//get batch file info by create time
//return total, []*FileInfoJson, error
func (f *Pond) GetFiles(
			page, pageSize int,
		) (int64, []*json.FileInfoJson, error) {
	return f.storage.GetFilesInfo(page, pageSize)
}

////////////////////
//api for file data
////////////////////

//del data
func (f *Pond) DelData(shortUrl string) error {
	return f.storage.DeleteData(shortUrl)
}

//read data
//extend para: offset, length
func (f *Pond) ReadData(
			shortUrl string,
			offsetAndLength ...int64,
		) ([]byte, error) {
	return f.storage.ReadData(shortUrl, offsetAndLength...)
}

//write new data, if assigned short url means overwrite data
//if overwrite data, fix chunk size config should be true
//return shortUrl, error
func (f *Pond) WriteData(
			data []byte,
			shortUrls ...string,
		) (string, error) {
	return f.storage.WriteData(data, shortUrls...)
}

///////////////////////
//api for config setup
///////////////////////

//set config, STEP-2
func (f *Pond) SetConfig(cfg *conf.Config) error {
	//check
	if cfg == nil || cfg.DataPath == "" {
		return errors.New("invalid parameter")
	}
	if cfg.ChunkSizeMax < define.DefaultChunkMaxSize {
		cfg.ChunkSizeMax = define.DefaultChunkMaxSize
	}
	if cfg.ChunkBlockSize <= 0 {
		cfg.ChunkBlockSize = define.DefaultChunkBlockSize
	}
	if cfg.FileActiveHours <= 0 {
		cfg.FileActiveHours = define.DefaultChunkActiveHours
	}
	if cfg.MinChunkFiles <= 0 {
		cfg.MinChunkFiles = define.DefaultMinChunkFiles
	}
	return f.storage.SetConfig(cfg, &_wg)
}

//set redis config, optional
func (f *Pond) SetRedisConfig(cfg *conf.RedisConfig) error {
	return f.storage.SetRedisConfig(cfg)
}

//gen new config, STEP-1
func (f *Pond) GenConfig() *conf.Config {
	return &conf.Config{
		ChunkSizeMax: define.DefaultChunkMaxSize,
		ChunkBlockSize: define.DefaultChunkBlockSize,
		FileActiveHours: define.DefaultChunkActiveHours,
	}
}

//gen redis config
func (f *Pond) GenRedisConfig() *conf.RedisConfig {
	return &conf.RedisConfig{
		KeyPrefix: define.DefaultKeyPrefix,
		FileInfoHashKeys: define.DefaultFileInfoHashKeys,
		FileBaseHashKeys: define.DefaultFileBaseHashKeys,
	}
}