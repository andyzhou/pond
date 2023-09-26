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
	maxChunkSize int64
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
		maxChunkSize: define.DefaultChunkMaxSize,
		objLocker:    sync.RWMutex{},
		tickChan: make(chan struct{}, 1),
		closeChan: make(chan bool, 1),
	}
	//go this.runMainProcess()
	return this
}

//close meta file
func (f *Meta) Quit() {
	//close main process
	if f.closeChan != nil {
		close(f.closeChan)
	}

	//save data
	err := f.SaveMeta()
	if err != nil {
		return
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
func (f *Meta) SaveMeta() error {
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

//get root path
func (f *Meta) GetRootPath() string {
	return f.rootPath
}

//set chunk max size
func (f *Meta) SetChunkMaxSize(size int64) error {
	if size <= 0 {
		return errors.New("invalid size parameter")
	}
	f.maxChunkSize = size
	return nil
}

//set root path and load gob file
func (f *Meta) SetRootPath(path string) error {
	//check
	if path == "" {
		return errors.New("invalid path parameter")
	}
	if f.metaFile != "" {
		return errors.New("path had setup")
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
	return err
}

/////////////////
//private func
/////////////////

//gen new file data id
func (f *Meta) genNewFileDataId() int64 {
	//gen new id
	newDataId := atomic.AddInt64(&f.metaJson.FileId, 1)
	//save meta data
	f.SaveMeta()
	return newDataId
}