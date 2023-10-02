package chunk

import (
	"errors"
	"fmt"
	"github.com/andyzhou/pond/conf"
	"github.com/andyzhou/pond/define"
	"github.com/andyzhou/pond/json"
	"log"
	"os"
	"sync"
	"sync/atomic"
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
	file *os.File
	chunkFileId int64
	metaFilePath string
	dataFilePath string
	blockSize int64
	readProcesses int
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
	if f.file != nil {
		//force update meta data
		f.updateMetaFile(true)
		f.file.Close()
		f.file = nil
	}
	if f.readCloseChan != nil {
		close(f.readCloseChan)
	}
	if f.writeCloseChan != nil {
		close(f.writeCloseChan)
	}
	if f.metaCloseChan != nil {
		close(f.metaCloseChan)
	}
}

//check active or not
func (f *Chunk) IsActive() bool {
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

/////////////////
//private func
/////////////////

//gen new file id
func (f *Chunk) genNewFileId() int64 {
	return atomic.AddInt64(&f.chunkObj.Id, 1)
}

//inter init
func (f *Chunk) interInit() {
	//load meta data
	err := f.loadMetaFile()
	if err != nil {
		log.Printf("chunk load meta file %v failed, err:%v\n", f.metaFilePath, err.Error())
	}

	//open data file
	err = f.openDataFile()
	if err != nil {
		log.Printf("chunk open data file %v failed, err:%v\n", f.dataFilePath, err.Error())
	}

	//run save meta process
	go f.saveMetaProcess()
}