package storage

import (
	"errors"
	"fmt"
	"github.com/andyzhou/pond/conf"
	"github.com/andyzhou/pond/define"
	"github.com/andyzhou/pond/face"
	"github.com/andyzhou/pond/json"
	"github.com/andyzhou/pond/util"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

/*
 * chunk meta file face
 * - all chunk info storage as one meta file
 */

//face info
type Meta struct {
	cfg *conf.Config //reference
	gob *face.Gob
	shortUrl *face.ShortUrl
	metaFile string
	metaJson *json.MetaJson //running data
	metaUpdated bool
	objLocker sync.RWMutex
	tickChan chan struct{}
	closeChan chan bool
	util.Util
	sync.RWMutex
}

//construct
func NewMeta() *Meta {
	//self init
	this := &Meta{
		metaJson:     json.NewMetaJson(),
		gob: face.NewGob(),
		shortUrl: face.NewShortUrl(),
		objLocker:    sync.RWMutex{},
		tickChan: make(chan struct{}, 1),
		closeChan: make(chan bool, 1),
	}
	return this
}

//close meta file
func (f *Meta) Quit() {
	//close main process
	if f.closeChan != nil {
		close(f.closeChan)
	}

	//force save data
	err := f.SaveMeta(true)
	if err != nil {
		log.Printf("meta.Quit err:%v\n", err.Error())
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

	//sync into meta obj with locker
	f.objLocker.Lock()
	defer f.objLocker.Unlock()
	atomic.AddInt64(&f.metaJson.FileId, 1)
	f.metaJson.Chunks = append(f.metaJson.Chunks, newChunkId)

	//save meta file
	f.SaveMeta()
	return newChunkFileObj
}

//save meta data
func (f *Meta) SaveMeta(isForces ...bool) error {
	var (
		isForce bool
	)

	//detect
	if isForces != nil && len(isForces) > 0 {
		isForce = isForces[0]
	}

	//check
	if f.cfg.LazyMode && !isForce {
		//do nothing, just update switcher
		f.metaUpdated = false
		return nil
	}

	//force save meta data
	err := f.saveMetaData()
	return err
}

//set config
func (f *Meta) SetConfig(
			cfg *conf.Config,
		) error {
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
	if err == nil {
		//start main process
		go f.runMainProcess()
	}
	return err
}

/////////////////
//private func
/////////////////

//run main process
func (f *Meta) runMainProcess() {
	var (
		m any = nil
	)

	//defer
	defer func() {
		if err := recover(); err != m {
			log.Printf("meta.mainProecss panic, err:%v\n", err)
		}
	}()

	//tick setup
	tick := func() {
		sf := func() {
			if f.tickChan != nil {
				f.tickChan <- struct{}{}
			}
		}
		duration := time.Duration(define.ChunksMetaSaveRate) * time.Second
		time.AfterFunc(duration, sf)
	}
	tick()

	//loop
	for {
		select {
		case <- f.tickChan:
			{
				//save meta
				f.autoSaveMeta()
				//start next tick
				tick()
			}
		case <- f.closeChan:
			return
		}
	}
}

//auto save meta
func (f *Meta) autoSaveMeta() {
	f.Lock()
	defer f.Unlock()
	if f.metaUpdated {
		//has updated, do nothing
		return
	}
	f.saveMetaData()
	f.metaUpdated = true
}

//save meta data
func (f *Meta) saveMetaData() error {
	//check
	if f.metaFile == "" {
		return errors.New("meta gob file not setup")
	}

	//begin save meta with locker
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