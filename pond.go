package pond

import (
	"errors"
	"sync"

	"github.com/andyzhou/pond/conf"
	"github.com/andyzhou/pond/define"
	"github.com/andyzhou/pond/json"
	"github.com/andyzhou/pond/storage"
)

/*
 * api interface
 * @author <AndyZhou>
 * @mail <diudiu8848@163.com>
 * - service one single node or group data
 * - one root data path, one pond obj
 */

//global variable
var (
	_pond     *Pond
	_pondOnce sync.Once
)

//face info
type Pond struct {
	storage  *storage.Storage
	wg       sync.WaitGroup
	initDone bool
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
	wg := sync.WaitGroup{}
	this := &Pond{
		wg: wg,
		storage: storage.NewStorage(&wg),
	}
	return this
}

//quit
func (f *Pond) Quit() {
	f.storage.Quit()
}

//get batch file info by create time
//return total, []*FileInfoJson, error
func (f *Pond) GetFiles(
		page, pageSize int,
	) (int64, []*json.FileInfoJson, error) {
	//check
	if !f.initDone {
		return 0, nil, errors.New("inter config not init")
	}
	return f.storage.GetFilesInfo(page, pageSize)
}

//del data
func (f *Pond) DelData(shortUrl string) error {
	//check
	if !f.initDone {
		return errors.New("inter config not init")
	}
	return f.storage.DeleteData(shortUrl)
}

//read data
//extend para: offset, length
func (f *Pond) ReadData(
		shortUrl string,
		offsetAndLength ...int64,
	) ([]byte, error) {
	//check
	if !f.initDone {
		return nil, errors.New("inter config not init")
	}
	return f.storage.ReadData(shortUrl, offsetAndLength...)
}

//write new data, if assigned short url means overwrite data
//if overwrite data, fix chunk size config should be true
//return shortUrl, error
func (f *Pond) WriteData(
		data []byte,
		shortUrls ...string,
	) (string, error) {
	//check
	if !f.initDone {
		return "", errors.New("inter config not init")
	}
	return f.storage.WriteData(data, shortUrls...)
}

//set config, STEP-2
func (f *Pond) SetConfig(
	cfg *conf.Config,
	redisCfg ...*conf.RedisConfig) error {
	//check
	if cfg == nil || cfg.DataPath == "" {
		return errors.New("invalid parameter")
	}

	//setup base config
	if cfg.ChunkBlockSize <= 0 {
		cfg.ChunkBlockSize = define.DefaultChunkBlockSize
	}
	if cfg.FileActiveHours <= 0 {
		cfg.FileActiveHours = define.DefaultChunkActiveHours
	}
	if cfg.MinChunkFiles <= 0 {
		cfg.MinChunkFiles = define.DefaultMinChunkFiles
	}

	//call inter func
	err := f.storage.SetConfig(cfg, redisCfg...)
	if err != nil {
		return err
	}

	f.initDone = true
	return nil
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