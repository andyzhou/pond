package storage

import (
	"errors"
	"github.com/andyzhou/pond/chunk"
	"github.com/andyzhou/pond/conf"
	"github.com/andyzhou/pond/define"
	"github.com/andyzhou/tinylib/queue"
	"sync"
	"sync/atomic"
	"time"
)

/*
 * inter data manager
 * @author <AndyZhou>
 * @mail <diudiu8848@163.com>
 * - meta, chunk data manage
 */

//face info
type Manager struct {
	wg *sync.WaitGroup //reference
	cfg *conf.Config //reference
	chunk *Chunk
	meta *Meta
	ticker *queue.Ticker
	chunkMap sync.Map //chunkId -> *Chunk, active chunk file map
	chunkMaxSize int64
	chunks int32 //atomic count
	initDone bool
	lazyMode bool
	sync.RWMutex
}

//construct
func NewManager(wg *sync.WaitGroup) *Manager {
	this := &Manager{
		wg: wg,
		chunk: NewChunk(),
		meta: NewMeta(wg),
		chunkMap: sync.Map{},
		chunkMaxSize: define.DefaultChunkMaxSize,
	}
	this.interInit()
	return this
}

//quit
func (f *Manager) Quit() {
	if f.ticker != nil {
		f.ticker.Quit()
	}
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
func (f *Manager) checkUnActiveChunkFiles() error {
	//check
	if f.chunks <= 0 {
		return errors.New("no any chunks")
	}
	//loop check
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
	return nil
}

//cb for ticker quit
func (f *Manager) cbForTickQuit() {
	if f.wg != nil {
		f.wg.Done()
	}
}

//inter init
func (f *Manager) interInit() {
	//init ticker
	f.ticker = queue.NewTicker(define.ManagerTickerSeconds * time.Second)
	f.ticker.SetQuitCallback(f.cbForTickQuit)
	f.ticker.SetCheckerCallback(f.checkUnActiveChunkFiles)

	//wait group add count
	if f.wg != nil {
		f.wg.Add(1)
	}
}