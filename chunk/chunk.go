package chunk

import (
	"encoding/gob"
	"errors"
	"fmt"
	"log"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/andyzhou/pond/conf"
	"github.com/andyzhou/pond/define"
	"github.com/andyzhou/pond/json"
	"github.com/andyzhou/tinylib/queue"
	"github.com/andyzhou/tinylib/util"
)

/*
 * one chunk file face
 * @author <AndyZhou>
 * @mail <diudiu8848@163.com>
 * - one chunk, one meta and data file
 * - read/write real file chunk data
 * - use queue mode for concurrency and performance
 */

//face info
type Chunk struct {
	cfg                 *conf.Config //reference
	chunkObj            *json.ChunkFileJson
	gob                 *util.Gob
	file                *os.File //chunk file handler
	data                []byte   //memory map data
	readQueue           *queue.Queue
	writeQueue          *queue.Queue
	metaTicker          *queue.Ticker
	chunkFileId         int64
	metaFilePath        string
	dataFilePath        string
	lastActiveTime      int64 //time stamp value
	openDone            bool
	metaUpdated         bool
	writeLazy, readLazy bool
	metaLocker          sync.RWMutex
	fileLocker          sync.RWMutex
}

//init for gob register
func init()  {
	gob.Register(&json.ChunkFileJson{})
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
		gob: util.NewGob(),
		chunkFileId: chunkFileId,
		metaFilePath: chunkMetaFile,
		dataFilePath: fmt.Sprintf("%v/%v/%v", cfg.DataPath, define.SubDirOfFile, chunkDataFile),
		readQueue: queue.NewQueue(),
		writeQueue: queue.NewQueue(),
	}

	//inter init
	this.interInit()
	return this
}

//quit
func (f *Chunk) Quit() {
	if f.writeQueue != nil {
		f.writeQueue.Quit()
	}
	if f.readQueue != nil {
		f.readQueue.Quit()
	}
	if f.metaTicker != nil {
		f.metaTicker.Quit()
	}

	//close opened data file
	f.CloseFile()
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

//get chunk active size
func (f *Chunk) GetChunkActiveSize() int64 {
	return f.chunkObj.Size
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

	//lazy mode check
	if f.cfg.ReadLazy {
		f.readLazy = true
	}
	if f.cfg.WriteLazy {
		f.writeLazy = true
	}

	//open file
	err := f.openDataFile()
	if err != nil {
		log.Printf("chunk file %v open failed, err:%v\n", f.dataFilePath, err.Error())
		panic(any(err))
	}

	//load meta data
	err = f.loadMetaFile()
	if err != nil {
		log.Printf("chunk load meta file %v failed, err:%v\n", f.metaFilePath, err.Error())
	}

	//set cb for read and write file queue
	f.readQueue.SetCallback(f.cbForReadOpt)
	f.writeQueue.SetCallback(f.cbForWriteOpt)

	//init meta ticker
	f.metaTicker = queue.NewTicker(define.DefaultChunkMetaTicker)
	f.metaTicker.SetCheckerCallback(f.cbForUpdateMeta)
}