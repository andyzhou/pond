package chunk

import (
	"errors"
	"fmt"
	"github.com/andyzhou/pond/conf"
	"github.com/andyzhou/pond/define"
	"github.com/andyzhou/pond/face"
	"github.com/andyzhou/pond/json"
	"log"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

/*
 * one chunk file face
 * - one chunk, one meta and data file
 * - read/write real file chunk data
 * - use queue mode for concurrency and performance
 */


//face info
type Chunk struct {
	cfg *conf.Config //reference
	chunkObj *json.ChunkFileJson
	gob *face.Gob
	file *os.File
	chunkFileId int64
	metaFilePath string
	dataFilePath string
	lastActiveTime int64 //time stamp value
	openDone bool
	metaUpdated bool
	isLazyMode bool
	readChan chan ReadReq
	writeChan chan WriteReq
	readCloseChan chan bool
	writeCloseChan chan bool
	metaCloseChan chan bool
	sync.RWMutex
}

//construct
func NewChunk(
			chunkFileId int64,
			cfg *conf.Config,
		) *Chunk {
	//format chunk file
	chunkDataFile := fmt.Sprintf(define.ChunkDataFilePara, chunkFileId)
	chunkMetaFile := fmt.Sprintf(define.ChunkMetaFilePara, chunkFileId)

	//self init
	this := &Chunk{
		cfg: cfg,
		gob: face.NewGob(),
		chunkFileId: chunkFileId,
		metaFilePath: chunkMetaFile,
		dataFilePath: fmt.Sprintf("%v/%v/%v", cfg.DataPath, define.SubDirOfFile, chunkDataFile),
		readChan: make(chan ReadReq, define.DefaultChunkChanSize),
		writeChan: make(chan WriteReq, define.DefaultChunkChanSize),
		readCloseChan: make(chan bool, 1),
		writeCloseChan: make(chan bool, 1),
		metaCloseChan: make(chan bool, 1),
	}
	this.interInit()
	return this
}

//quit
func (f *Chunk) Quit() {
	//close opened data file
	f.closeDataFile()

	//meta close chan
	if f.metaCloseChan != nil {
		close(f.metaCloseChan)
	}
}

//check file opened or not
func (f *Chunk) IsOpened() bool {
	return f.openDone
}

//check file active time is available
func (f *Chunk) IsActive() bool {
	now := time.Now().Unix()
	diff := now - f.lastActiveTime
	return diff <= int64(f.cfg.FileActiveHours * define.SecondsOfHour)
}

//check size is available or not
func (f *Chunk) IsAvailable() bool {
	return f.chunkObj.Size < f.chunkObj.MaxSize
}

//set chunk max size
func (f *Chunk) SetChunkMaxSize(size int64) error {
	if size <= 0 {
		return errors.New("invalid size parameter")
	}
	f.chunkObj.MaxSize = size
	f.updateMetaFile()
	return nil
}

//get file id
func (f *Chunk) GetFileId() int64 {
	return f.chunkObj.Id
}

//open、close relate files
func (f *Chunk) OpenFile() error {
	return f.openDataFile()
}
func (f *Chunk) CloseFile() error {
	return f.closeDataFile()
}

/////////////////
//private func
/////////////////

//gen new file id
func (f *Chunk) genNewFileId() int64 {
	return atomic.AddInt64(&f.chunkObj.Id, 1)
}

//inter init
func (f *Chunk) interInit() {
	//init gob
	rootPath := fmt.Sprintf("%v/%v", f.cfg.DataPath, define.SubDirOfFile)
	f.gob.SetRootPath(rootPath)

	//load meta data
	err := f.loadMetaFile()
	if err != nil {
		log.Printf("chunk load meta file %v failed, err:%v\n", f.metaFilePath, err.Error())
	}

	////open data file
	//err = f.openDataFile()
	//if err != nil {
	//	log.Printf("chunk open data file %v failed, err:%v\n", f.dataFilePath, err.Error())
	//}

	//run save meta process
	go f.saveMetaProcess()
}