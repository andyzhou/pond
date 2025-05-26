package storage

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/andyzhou/pond/chunk"
	"github.com/andyzhou/pond/conf"
	"github.com/andyzhou/pond/data"
	"github.com/andyzhou/pond/define"
)

/*
 * inter data manager
 * @author <AndyZhou>
 * @mail <diudiu8848@163.com>
 * - meta, chunk data manage
 */

//face info
type Manager struct {
	wg           *sync.WaitGroup      //reference
	cfg          *conf.Config         //reference
	data         *data.InterRedisData //reference
	chunk        *Chunk
	meta         *Meta
	chunkMap     sync.Map //chunkId -> *Chunk, active chunk file map
	chunkMaxSize int64
	chunks       int32 //atomic count
	useRedis     bool
	initDone     bool
	lazyMode     bool
	sync.RWMutex
}

//construct
func NewManager(wg *sync.WaitGroup) *Manager {
	this := &Manager{
		wg: wg,
		chunk: NewChunk(wg),
		meta: NewMeta(wg),
		chunkMap: sync.Map{},
		chunkMaxSize: define.DefaultChunkMaxSize,
	}
	this.interInit()
	return this
}

//quit
func (f *Manager) Quit() {
	//inter obj quit
	f.meta.Quit()
	f.chunk.Quit()

	//clean chunk map
	sf := func(k, v interface{}) bool {
		chunkObj, _ := v.(*chunk.Chunk)
		if chunkObj != nil {
			chunkObj.Quit()
		}
		return true
	}
	f.chunkMap.Range(sf)

	//wait group done
	if f.wg != nil {
		f.wg.Done()
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

//get all chunk active size
//return map[chunkId]activeSize
func (f *Manager) GetAllChunkSize() map[int64]int64 {
	//check
	if &f.chunkMap == nil {
		return nil
	}

	//format result
	result := make(map[int64]int64)
	sf := func(k, v interface{}) bool {
		chunkId, _ := k.(int64)
		chunkObj, _ := v.(*chunk.Chunk)
		if chunkId > 0 && chunkObj != nil {
			result[chunkId] = chunkObj.GetChunkActiveSize()
		}
		return true
	}
	f.chunkMap.Range(sf)
	return result
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
	//f.Lock()
	//defer f.Unlock()
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

//init new chunk info
//return newChunkId
func (f *Manager) InitNewChunk() int64 {
	//create new with locker
	//f.Lock()
	//defer f.Unlock()

	//begin create new
	chunkFileObj := f.meta.CreateNewChunk()
	chunkId := chunkFileObj.Id

	//init chunk face
	target := chunk.NewChunk(chunkId, f.cfg)
	target.SetChunkMaxSize(f.chunkMaxSize)

	//storage into run map
	f.chunkMap.Store(chunkId, target)
	return chunkId
}

//set config
func (f *Manager) SetConfig(cfg *conf.Config, userRedis ...bool) error {
	var (
		needUserRedis bool
	)
	//check
	if cfg == nil || cfg.DataPath == "" {
		return errors.New("invalid parameter")
	}
	if f.initDone {
		return nil
	}
	if userRedis != nil && len(userRedis) > 0 {
		needUserRedis = userRedis[0]
	}

	//sync env
	f.cfg = cfg
	f.chunkMaxSize = cfg.ChunkSizeMax
	f.useRedis = needUserRedis
	f.chunk.SetUseRedis(needUserRedis)

	//init meta
	err := f.meta.SetConfig(cfg)
	if err != nil {
		return err
	}

	//set chunk config
	f.chunk.SetConfig(cfg)

	//defer
	defer func() {
		f.initDone = true
	}()

	//load meta data obj into run env
	metaObj := f.meta.GetMetaData()
	chunks := int32(0)
	if metaObj != nil && metaObj.ChunkId > 0 {
		//loop init old chunk obj
		for _, chunkId := range metaObj.Chunks {
			//init chunk face
			chunkObj := chunk.NewChunk(chunkId, f.cfg)

			//storage into run map
			f.chunkMap.Store(chunkId, chunkObj)
			chunks++
		}
		//update chunk count
		atomic.StoreInt32(&f.chunks, chunks)
	}else{
		//pre-create batch empty chunk files
		for i := 1; i <= cfg.MinChunkFiles; i++ {
			//create new chunk info
			f.InitNewChunk()

			////init chunk face
			//chunkObj := chunk.NewChunk(chunkId, f.cfg)
			//
			////storage into run map
			//f.chunkMap.Store(chunkId, chunkObj)
			chunks++
			time.Sleep(time.Second/5)
		}
		//update chunk count
		atomic.StoreInt32(&f.chunks, chunks)

		//force save meta data
		f.meta.SaveMeta(true)
	}
	return err
}

//set data obj
func (f *Manager) SetData(data *data.InterRedisData) {
	f.data = data
	f.chunk.SetData(data)
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

////cb for ticker quit
//func (f *Manager) cbForTickQuit() {
//	if f.wg != nil {
//		f.wg.Done()
//		log.Println("pond.manager.cbForTickQuit")
//	}
//}
//
////inter chunk files check ticker
//func (f *Manager) startChunkFilesChecker() {
//	//init ticker
//	f.ticker = queue.NewTicker(define.ManagerTickerSeconds)
//	f.ticker.SetCheckerCallback(f.checkUnActiveChunkFiles)
//	f.ticker.SetQuitCallback(f.cbForTickQuit)
//}

//inter init
func (f *Manager) interInit() {
	//start chunk files checker
	//f.startChunkFilesChecker()

	//wait group add count
	if f.wg != nil {
		f.wg.Add(1)
	}
}