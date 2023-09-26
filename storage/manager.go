package storage

import (
	"errors"
	"sync"
)

/*
 * inter data manager
 * - meta, chunk data manage
 */

//face info
type Manager struct {
	meta *Meta
	chunkMap sync.Map //chunkId -> *Chunk
	chunkIds []int64 //used for fast rand pick
	initDone bool
}

//construct
func NewManager() *Manager {
	this := &Manager{
		meta: NewMeta(),
		chunkMap: sync.Map{},
		chunkIds: []int64{},
	}
	return this
}

//quit
func (f *Manager) Quit() {
	f.meta.Quit()
	sf := func(k, v interface{}) bool {
		chunkObj, _ := v.(*Chunk)
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

//get active chunk ids
func (f *Manager) GetChunkIds() []int64 {
	return f.chunkIds
}

//get chunk obj by id
//used for read data
func (f *Manager) GetChunkById(id int64) (*Chunk, error) {
	//check
	if id <= 0 {
		return nil, errors.New("invalid parameter")
	}
	//load by id
	v, ok := f.chunkMap.Load(id)
	if ok && v != nil {
		return v.(*Chunk), nil
	}
	return nil, errors.New("no chunk obj")
}

//get active or create new chunk obj
//used for write data
func (f *Manager) GetActiveChunk() (*Chunk, error) {
	var (
		target *Chunk
	)

	//get active chunk data
	chunkMetaObj := f.meta.GetMetaData()
	if chunkMetaObj != nil {
		//get active chunk
		sf := func(k, v interface{}) bool {
			chunkObj, _ := v.(*Chunk)
			if chunkObj != nil && chunkObj.IsActive() {
				//found it
				target = chunkObj
				return false
			}
			return true
		}
		f.chunkMap.Range(sf)
	}
	if chunkMetaObj == nil || target == nil {
		//try create new
		chunkFileObj := f.meta.CreateNewChunk()
		chunkId := chunkFileObj.Id

		//init chunk face
		rootPath := f.meta.GetRootPath()
		target = NewChunk(rootPath, chunkId)

		//storage into run map
		f.chunkMap.Store(chunkId, target)
		f.chunkIds = append(f.chunkIds, chunkId)
	}
	if target == nil {
		return target, errors.New("can't get active chunk")
	}
	return target, nil
}

//set root path
func (f *Manager) SetRootPath(path string) error {
	//check
	if f.initDone {
		return errors.New("path had init")
	}

	//init meta
	err := f.meta.SetRootPath(path)
	if err != nil {
		return err
	}

	//defer
	defer func() {
		f.initDone = true
	}()

	//load meta data obj into run env
	metaObj := f.meta.GetMetaData()
	if metaObj != nil {
		for _, chunkId := range metaObj.Chunks {
			//init chunk face
			chunkObj := NewChunk(f.meta.GetRootPath(), chunkId)

			//storage into run map
			f.chunkMap.Store(chunkId, chunkObj)
			f.chunkIds = append(f.chunkIds, chunkId)
		}
	}
	return err
}