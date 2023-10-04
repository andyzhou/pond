package storage

import (
	"errors"
	"github.com/andyzhou/pond/chunk"
	"github.com/andyzhou/pond/conf"
	"github.com/andyzhou/pond/define"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

/*
 * inter data manager
 * - meta, chunk data manage
 */

//face info
type Manager struct {
	cfg *conf.Config //reference
	chunk *Chunk
	meta *Meta
	chunkMap sync.Map //chunkId -> *Chunk, active chunk file map
	chunkMaxSize int64
	chunks int32 //atomic count
	initDone bool
	lazyMode bool
	tickChan chan struct{}
	closeChan chan bool
	sync.RWMutex
}

//construct
func NewManager() *Manager {
	this := &Manager{
		chunk: NewChunk(),
		meta: NewMeta(),
		chunkMap: sync.Map{},
		chunkMaxSize: define.DefaultChunkMaxSize,
		tickChan: make(chan struct{}, 1),
		closeChan: make(chan bool, 1),
	}
	go this.runMainProcess()
	return this
}

//quit
func (f *Manager) Quit() {
	f.meta.Quit()
	sf := func(k, v interface{}) bool {
		chunkObj, _ := v.(*chunk.Chunk)
		if chunkObj != nil {
			chunkObj.Quit()
		}
		return true
	}
	f.chunkMap.Range(sf)
	if f.closeChan != nil {
		close(f.closeChan)
	}
}

//gen new file short url
func (f *Manager) GenNewShortUrl() (string, error) {
	return f.meta.GenNewShortUrl()
}

//get chunk obj by id
//used for read data
func (f *Manager) GetChunkById(id int64) (*chunk.Chunk, error) {
	//check
	if id <= 0 {
		return nil, errors.New("invalid parameter")
	}
	//load by id
	v, ok := f.chunkMap.Load(id)
	if ok && v != nil {
		return v.(*chunk.Chunk), nil
	}
	return nil, errors.New("no chunk obj")
}

//get running chunk obj
func (f *Manager) GetRunningChunk() *Chunk {
	return f.chunk
}

//get active or create new chunk obj
//used for write data
func (f *Manager) GetActiveChunk() (*chunk.Chunk, error) {
	var (
		target *chunk.Chunk
	)

	//get active chunk data with locker
	f.Lock()
	defer f.Unlock()
	if f.chunks > 0 {
		//get active chunk
		sf := func(k, v interface{}) bool {
			chunkObj, _ := v.(*chunk.Chunk)
			if chunkObj != nil && chunkObj.IsAvailable() {
				//found it
				target = chunkObj
				return false
			}
			return true
		}
		f.chunkMap.Range(sf)
	}
	if target == nil {
		//try create new
		chunkFileObj := f.meta.CreateNewChunk()
		chunkId := chunkFileObj.Id

		//init chunk face
		target = chunk.NewChunk(chunkId, f.cfg)
		target.SetChunkMaxSize(f.chunkMaxSize)

		//storage into run map
		f.chunkMap.Store(chunkId, target)
	}
	if target == nil {
		return target, errors.New("can't get active chunk")
	}
	return target, nil
}

//set config
func (f *Manager) SetConfig(cfg *conf.Config) error {
	//check
	if cfg == nil || cfg.DataPath == "" {
		return errors.New("invalid parameter")
	}
	if f.initDone {
		return nil
	}
	f.cfg = cfg
	f.chunkMaxSize = cfg.ChunkSizeMax

	//init meta
	err := f.meta.SetConfig(cfg)
	if err != nil {
		return err
	}

	//defer
	defer func() {
		f.initDone = true
	}()

	//load removed chunk info
	f.chunk.Load()

	//load meta data obj into run env
	metaObj := f.meta.GetMetaData()
	if metaObj != nil {
		//loop init old chunk obj
		chunks := int32(0)
		for _, chunkId := range metaObj.Chunks {
			//init chunk face
			chunkObj := chunk.NewChunk(chunkId, f.cfg)

			//storage into run map
			f.chunkMap.Store(chunkId, chunkObj)
			chunks++
		}
		//update chunk count
		atomic.StoreInt32(&f.chunks, chunks)
	}
	return err
}

////////////////
//private func
////////////////

//check un-active chunk files
func (f *Manager) checkUnActiveChunkFiles() {
	if f.chunks <= 0 {
		return
	}
	sf := func(k, v interface{}) bool {
		chunkObj, _ := v.(*chunk.Chunk)
		if chunkObj != nil &&
			chunkObj.IsOpened() &&
			!chunkObj.IsActive() {
			//close un-active chunk file
			chunkObj.CloseFile()
		}
		return true
	}
	f.chunkMap.Range(sf)
}

//main process for check chunk files
func (f *Manager) runMainProcess() {
	var (
		m any = nil
	)
	//defer
	defer func() {
		if err := recover(); err != m {
			log.Printf("manager.mainProcess panic, err:%v\n", err)
		}
	}()

	//ticker
	ticker := func() {
		sf := func() {
			if f.tickChan != nil {
				f.tickChan <- struct{}{}
			}
		}
		time.AfterFunc(define.ManagerTickerSeconds * time.Second, sf)
	}

	//start first ticker
	ticker()

	//loop
	for {
		select {
		case <- f.tickChan:
			{
				//check
				f.checkUnActiveChunkFiles()
				//start next ticker
				ticker()
			}
		case <- f.closeChan:
			return
		}
	}
}