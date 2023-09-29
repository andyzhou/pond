package storage

import (
	"errors"
	"fmt"
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
	rootPath string
	metaFile string
	metaJson *json.MetaJson //running data
	metaUpdated bool
	lazySaveMode bool
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
	shortUrl, err := face.GetFace().GetShortUrl().Generator(inputVal)
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
	if f.lazySaveMode && !isForce {
		//do nothing, just update switcher
		f.metaUpdated = false
		return nil
	}

	//force save meta data
	err := f.saveMetaData()
	return err
}

//get root path
func (f *Meta) GetRootPath() string {
	return f.rootPath
}

//set lazy save meta file mode
func (f *Meta) SetLazyMode(switcher bool) {
	//check
	if (switcher && f.lazySaveMode) || (!switcher && !f.lazySaveMode) {
		//same value, do nothing
		return
	}
	if switcher && !f.lazySaveMode {
		//start lazy mode
		f.lazySaveMode = true
		go f.runMainProcess()
		return
	}
	//close lazy mode
	f.lazySaveMode = false
	f.closeChan <- true
}

//set root path and load gob file
func (f *Meta) SetRootPath(
			path string,
			isLazyModes ...bool,
		) error {
	//check
	if path == "" {
		return errors.New("invalid path parameter")
	}
	if f.metaFile != "" {
		return errors.New("path had setup")
	}

	//detect
	if isLazyModes != nil && len(isLazyModes) > 0 {
		f.lazySaveMode = isLazyModes[0]
	}

	//format file root path
	f.rootPath = fmt.Sprintf("%v/%v", path, define.SubDirOfFile)

	//check and create sub dir
	err := f.CheckDir(f.rootPath)
	if err != nil {
		return err
	}

	//setup meta path and file
	gob := face.GetFace().GetGob()
	gob.SetRootPath(f.rootPath)
	f.metaFile = define.ChunksMetaFile

	//check and load meta file
	err = gob.Load(f.metaFile, &f.metaJson)
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

	//get gob face
	gob := face.GetFace().GetGob()

	//begin save meta with locker
	err := gob.Store(f.metaFile, f.metaJson)
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