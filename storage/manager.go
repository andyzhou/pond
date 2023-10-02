package storage

import (
	"errors"
	"github.com/andyzhou/pond/chunk"
	"github.com/andyzhou/pond/conf"
	"github.com/andyzhou/pond/define"
	"sync"
	"sync/atomic"
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
	freeFileMap sync.Map //md5 -> *json.FileBaseJson
	chunkMap sync.Map //chunkId -> *Chunk, active chunk file map
	chunkMaxSize int64
	chunks int32 //atomic count
	initDone bool
	lazyMode bool
	sync.RWMutex
}

//construct
func NewManager() *Manager {
	this := &Manager{
		chunk: NewChunk(),
		meta: NewMeta(),
		freeFileMap: sync.Map{},
		chunkMap: sync.Map{},
		chunkMaxSize: define.DefaultChunkMaxSize,
	}
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

//get

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
			if chunkObj != nil && chunkObj.IsActive() {
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