package storage

import (
	"errors"
	"fmt"
	"github.com/andyzhou/pond/define"
	"github.com/andyzhou/pond/face"
	"github.com/andyzhou/pond/json"
	"log"
	"math"
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

//inter struct
type (
	//read req
	ChunkReadReq struct {
		Offset int64
		Size int64
		Resp chan ChunkReadResp
	}
	ChunkReadResp struct {
		Data []byte
		Err error
	}

	//write req
	ChunkWriteReq struct {
		Data []byte
		Offset int64 //assigned offset for overwrite
		Resp chan ChunkWriteResp
	}
	ChunkWriteResp struct {
		NewOffSet int64
		BlockSize int64
		Err error
	}
)

//face info
type Chunk struct {
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
	readChan chan ChunkReadReq
	writeChan chan ChunkWriteReq
	readCloseChan chan bool
	writeCloseChan chan bool
	metaCloseChan chan bool
	sync.RWMutex
}

//construct
func NewChunk(
			rootPath string,
			chunkFileId int64,
			lazyMode bool,
			readProcesses ...int,
		) *Chunk {
	//detect read process number
	var (
		readProcess int
	)
	if readProcesses != nil && len(readProcesses) > 0 {
		readProcess = readProcesses[0]
	}
	if readProcess <= 0 {
		readProcess = define.DefaultChunkReadProcess
	}

	//format chunk file
	chunkDataFile := fmt.Sprintf(define.ChunkDataFilePara, chunkFileId)
	chunkMetaFile := fmt.Sprintf(define.ChunkMetaFilePara, chunkFileId)

	//self init
	this := &Chunk{
		chunkFileId: chunkFileId,
		metaFilePath: chunkMetaFile,
		dataFilePath: fmt.Sprintf("%v/%v", rootPath, chunkDataFile),
		blockSize: define.DefaultChunkBlockSize,
		isLazyMode: lazyMode,
		readProcesses: readProcess,
		readChan: make(chan ChunkReadReq, define.DefaultChunkChanSize),
		writeChan: make(chan ChunkWriteReq, define.DefaultChunkChanSize),
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
	return f.chunkObj.Size < define.DefaultChunkMaxSize
}

//get file id
func (f *Chunk) GetFileId() int64 {
	return f.chunkObj.Id
}

//read file
func (f *Chunk) ReadFile(
			offset,
			size int64,
		) ([]byte, error) {
	//check
	if offset < 0 || size <= 0 {
		return nil, errors.New("invalid parameter")
	}

	//check lazy mode
	if !f.isLazyMode {
		//direct read data
		return f.directReadData(offset, size)
	}

	//for lazy mode
	if f.readChan == nil ||
		len(f.readChan) >= define.DefaultChunkChanSize {
		return nil, errors.New("chunk data read chan invalid")
	}

	//init read request
	req := ChunkReadReq{
		Offset: offset,
		Size: size,
		Resp: make(chan ChunkReadResp, 1),
	}

	//send request
	f.readChan <- req

	//wait for response
	resp, isOk := <- req.Resp
	if !isOk && &resp == nil {
		return nil, errors.New("can't get response data")
	}
	return resp.Data, resp.Err
}

//write file
//return ChunkWriteResp, error
func (f *Chunk) WriteFile(
			data []byte,
			offsets ...int64,
		) *ChunkWriteResp {
	var (
		offset int64 = -1
		resp ChunkWriteResp
	)

	//check
	if data == nil {
		resp.Err = errors.New("invalid parameter")
		return &resp
	}

	//check lazy mode
	if !f.isLazyMode {
		//direct write data
		return f.directWriteData(data, offsets...)
	}

	if f.writeChan == nil || len(f.writeChan) >= define.DefaultChunkChanSize {
		resp.Err = errors.New("chunk data write chan invalid")
		return &resp
	}

	//detect offset
	if offsets != nil && len(offsets) > 0 {
		offset = offsets[0]
	}

	//init write request
	req := ChunkWriteReq{
		Offset: offset,
		Data: data,
		Resp: make(chan ChunkWriteResp, 1),
	}

	//send request
	f.writeChan <- req

	//wait for response
	resp, isOk := <- req.Resp
	if !isOk && &resp == nil {
		resp = ChunkWriteResp{}
		resp.Err = errors.New("can't get response data")
		return &resp
	}
	return &resp
}

/////////////////
//private func
/////////////////

//read process
func (f *Chunk) readProcess() {
	var (
		req  ChunkReadReq
		isOk bool
		m    any = nil
	)

	//defer
	defer func() {
		if err := recover(); err != m {
			log.Printf("chunk.readProcess panic, err:%v\n", err)
		}
		//close chan
		close(f.readChan)
	}()

	//loop
	for {
		select {
		case req, isOk = <- f.readChan:
			if isOk {
				//read data
				data, err := f.readData(&req)

				//send response
				resp := ChunkReadResp{
					Data: data,
					Err: err,
				}
				req.Resp <- resp
			}
		case <- f.readCloseChan:
			{
				return
			}
		}
	}
}

//write process
func (f *Chunk) writeProcess() {
	var (
		req  ChunkWriteReq
		isOk bool
		m    any = nil
	)

	//defer
	defer func() {
		if err := recover(); err != m {
			log.Printf("chunk.writeProcess panic, err:%v\n", err)
		}

		//close chan
		close(f.writeChan)
	}()

	//loop
	for {
		select {
		case req, isOk = <- f.writeChan:
			if isOk {
				//write data
				resp := f.writeData(&req)
				//send response
				req.Resp <- *resp
			}
		case <- f.writeCloseChan:
			return
		}
	}
}

//meta auto save process
func (f *Chunk) saveMetaProcess() {
	var (
		ticker = time.NewTicker(define.ChunkFileMetaSaveRate * time.Second)
		m any = nil
	)

	//defer
	defer func() {
		if err := recover(); err != m {
			log.Printf("chunk.saveMetaProcess panic, err:%v\n", err)
		}

		//force save meta data
		f.updateMetaFile(true)

		//close ticker
		ticker.Stop()
	}()

	//loop
	for {
		select {
		case <- ticker.C:
			{
				if !f.metaUpdated {
					f.updateMetaFile()
				}
			}
		case <- f.metaCloseChan:
			return
		}
	}
}

//gen new file id
func (f *Chunk) genNewFileId() int64 {
	return atomic.AddInt64(&f.chunkObj.Id, 1)
}

//read file data
func (f *Chunk) readData(req *ChunkReadReq) ([]byte, error) {
	//check
	if req == nil || req.Offset < 0 || req.Size <= 0 {
		return nil, errors.New("invalid parameter")
	}
	return f.directReadData(req.Offset, req.Size)
}

//direct read file data
func (f *Chunk) directReadData(offset, size int64) ([]byte, error) {
	//check
	if offset < 0 || size <= 0 {
		return nil, errors.New("invalid parameter")
	}
	if f.file == nil {
		return nil, errors.New("chunk file closed")
	}

	//create block buffer
	byteData := make([]byte, size)

	//read real data
	_, err := f.file.ReadAt(byteData, offset)
	return byteData, err
}

//write file data
func (f *Chunk) writeData(req *ChunkWriteReq) *ChunkWriteResp {
	var (
		resp ChunkWriteResp
	)
	//check
	if req == nil || req.Data == nil {
		resp.Err = errors.New("invalid parameter")
		return &resp
	}
	if f.file == nil {
		resp.Err = errors.New("chunk file closed")
		return &resp
	}

	//begin write data
	writeResp := f.directWriteData(req.Data)
	resp = *writeResp
	return &resp
}

//direct write data
func (f *Chunk) directWriteData(
			data []byte,
			offsets ...int64,
		) *ChunkWriteResp {
	var (
		resp ChunkWriteResp
	)

	//check
	if data == nil {
		resp.Err = errors.New("invalid parameter")
		return &resp
	}
	if f.file == nil {
		resp.Err = errors.New("chunk file closed")
		return &resp
	}

	//calculate real block size
	dataSize := float64(len(data))
	realBlocks := int64(math.Ceil(dataSize / float64(f.blockSize)))

	//create block buffer
	realBlockSize := realBlocks * f.blockSize
	byteData := make([]byte, realBlockSize)
	copy(byteData, data)

	//write block buffer data into chunk
	_, err := f.file.WriteAt(byteData, f.chunkObj.Size)
	chunkOldSize := f.chunkObj.Size
	if err == nil {
		//update chunk obj
		f.chunkObj.Files++
		f.chunkObj.Size += realBlockSize

		//update meta file
		f.updateMetaFile()
	}

	//format resp
	resp.Err = err
	resp.NewOffSet = chunkOldSize
	resp.BlockSize = realBlockSize
	return &resp
}

//update meta file
func (f *Chunk) updateMetaFile(isForces ...bool) error {
	var (
		isForce bool
	)
	//check
	if f.metaFilePath == "" || f.chunkObj == nil {
		return errors.New("inter data not init yet")
	}

	//detect
	if isForces != nil && len(isForces) > 0 {
		isForce = isForces[0]
	}
	if !isForce {
		//just update switcher
		f.metaUpdated = false
		return nil
	}

	//force save meta data
	gob := face.GetFace().GetGob()
	err := gob.Store(f.metaFilePath, f.chunkObj)
	if err != nil {
		log.Printf("chunk.writeData, update meta failed, err:%v\n", err.Error())
	}
	f.metaUpdated = true
	return err
}

//open chunk data file
func (f *Chunk) openDataFile() error {
	//open real file, auto create if not exists
	file, err := os.OpenFile(f.dataFilePath, os.O_CREATE|os.O_RDWR, define.FilePerm)
	if err != nil {
		return err
	}

	//sync file handle
	f.file = file
	f.openDone = true

	if f.isLazyMode {
		//start write process
		go f.writeProcess()

		//start read process
		go f.readProcess()
	}
	return nil
}

//load chunk meta file
func (f *Chunk) loadMetaFile() error {
	//load god file
	gob := face.GetFace().GetGob()
	chunkObj := json.NewChunkFileJson()
	err := gob.Load(f.metaFilePath, &chunkObj)
	if err != nil {
		return err
	}

	//sync chunk obj
	f.chunkObj = chunkObj
	if f.chunkObj != nil && f.chunkObj.Id <= 0 {
		f.chunkObj.Id = f.chunkFileId
		f.updateMetaFile()
	}
	return nil
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