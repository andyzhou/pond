package storage

import (
	"encoding/gob"
	"errors"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/andyzhou/pond/conf"
	"github.com/andyzhou/pond/define"
	"github.com/andyzhou/pond/json"
	"github.com/andyzhou/pond/utils"
	"github.com/andyzhou/tinylib/queue"
	"github.com/andyzhou/tinylib/util"
)

/*
 * chunk meta file face
 * @author <AndyZhou>
 * @mail <diudiu8848@163.com>
 * - all chunk info storage as one meta file
 */

//face info
type Meta struct {
	wg          *sync.WaitGroup //reference
	cfg         *conf.Config    //reference
	gob         *util.Gob
	shortUrl    *util.ShortUrl
	ticker      *queue.Ticker
	metaFile    string
	metaJson    *json.MetaJson //running data
	metaUpdated bool
	objLocker   sync.RWMutex
	util.Util
	utils.Utils
	sync.RWMutex
}

//init
func init() {
	gob.Register(&json.MetaJson{})
}

//construct
func NewMeta(wg *sync.WaitGroup) *Meta {
	//self init
	this := &Meta{
		wg: wg,
		metaJson:  json.NewMetaJson(),
		gob:       util.NewGob(),
		shortUrl:  util.NewShortUrl(),
		objLocker: sync.RWMutex{},
	}
	this.interInit()
	return this
}

//close meta file
func (f *Meta) Quit() {
	defer func() {
		//force save data
		err := f.SaveMeta(true)
		if err != nil {
			log.Printf("meta.Quit err:%v\n", err.Error())
		}
	}()
	if f.ticker != nil {
		f.ticker.Quit()
	}
}

//gen new data short url
func (f *Meta) GenNewShortUrl() (string, error) {
	newDataId := f.genNewFileDataId()
	inputVal := fmt.Sprintf("%v:%v", newDataId, time.Now().UnixNano())
	shortUrl, err := f.shortUrl.Generator(inputVal)
	return shortUrl, err
}

//get meta data
func (f *Meta) GetMetaData() *json.MetaJson {
	return f.metaJson
}

//create new chunk file data
func (f *Meta) CreateNewChunk() *json.ChunkFileJson {
	//init new chunk obj
	newChunkId := atomic.AddInt64(&f.metaJson.ChunkId, 1)
	newChunkFileObj := json.NewChunkFileJson()
	newChunkFileObj.Id = newChunkId

	//defer
	defer func() {
		//save meta file
		f.SaveMeta(true)
	}()

	//sync into meta obj with locker
	f.objLocker.Lock()
	defer f.objLocker.Unlock()
	atomic.AddInt64(&f.metaJson.FileId, 1)
	f.metaJson.Chunks = append(f.metaJson.Chunks, newChunkId)

	return newChunkFileObj
}

//save meta data
func (f *Meta) SaveMeta(
	isForces ...bool) error {
	var (
		isForce bool
	)

	//detect
	if isForces != nil && len(isForces) > 0 {
		isForce = isForces[0]
	}

	//check
	if !isForce {
		//do nothing, just update switcher
		f.Lock()
		defer f.Unlock()
		f.metaUpdated = false
		return nil
	}

	//force save meta data
	err := f.saveMetaData()
	return err
}

//set config
func (f *Meta) SetConfig(
	cfg *conf.Config) error {
	//check
	if cfg == nil || cfg.DataPath == "" {
		return errors.New("invalid parameter")
	}
	if f.metaFile != "" {
		return errors.New("path had setup")
	}
	f.cfg = cfg

	//format file root path
	rootPath := fmt.Sprintf("%v/%v", cfg.DataPath, define.SubDirOfFile)

	//check and create sub dir
	err := f.CheckDir(rootPath)
	if err != nil {
		return err
	}

	//setup meta path and file
	f.gob.SetRootPath(rootPath)
	f.metaFile = define.ChunksMetaFile

	//check and load meta file
	err = f.gob.Load(f.metaFile, &f.metaJson)
	if err != nil {
		f.metaJson = json.NewMetaJson()
	}
	return nil
}

/////////////////
//private func
/////////////////

//save meta data
func (f *Meta) saveMetaData() error {
	//check
	if f.metaFile == "" {
		return errors.New("meta gob file not setup")
	}

	//begin save meta with locker
	f.Lock()
	defer f.Unlock()
	err := f.gob.Store(f.metaFile, f.metaJson)
	if err != nil {
		log.Printf("meta.SaveMeta failed, err:%v\n", err.Error())
	}
	return err
}

//gen new file data id
func (f *Meta) genNewFileDataId() int64 {
	//gen new id
	newDataId := atomic.AddInt64(&f.metaJson.FileId, 1)
	//save meta data
	f.SaveMeta()
	return newDataId
}

//cb for auto save meta
func (f *Meta) cbForAutoSaveMeta(inputs ...interface{}) error {
	if f.metaUpdated {
		//has updated, do nothing
		return errors.New("meta had updated")
	}
	f.saveMetaData()
	f.metaUpdated = true
	return nil
}

//for for tick quit
func (f *Meta) cbForTickQuit() {
	if f.wg != nil {
		f.wg.Done()
		log.Println("pond.meta.cbForTickQuit")
	}
}

//inter init
func (f *Meta) interInit() {
	//init ticker
	f.ticker = queue.NewTicker(define.MetaAutoSaveTicker)
	f.ticker.SetCheckerCallback(f.cbForAutoSaveMeta)
	f.ticker.SetQuitCallback(f.cbForTickQuit)

	//add wait group
	if f.wg != nil {
		f.wg.Add(1)
	}
}