package storage

import (
	"errors"
	"github.com/andyzhou/pond/conf"
	"sync"

	"github.com/andyzhou/pond/data"
	"github.com/andyzhou/pond/json"
)

/*
 * chunk base face
 * @author <AndyZhou>
 * @mail <diudiu8848@163.com>
 * - mark removed and re-use logic
 */

//inter data type
type (
	removedBaseFile struct {
		md5    string
		blocks int64
	}
)

//face info
type Chunk struct {
	cfg          *conf.Config //reference
	removed 	 *Removed
	Base
	sync.RWMutex
}

//construct
func NewChunk(wg *sync.WaitGroup) *Chunk {
	this := &Chunk{
		removed: NewRemoved(wg),
	}
	return this
}

//quit
func (f *Chunk) Quit() {
	f.removed.Quit()
}

////remove file base info
//func (f *Chunk) RemoveRemovedFileBase(md5 string) error {
//	var (
//		err error
//	)
//	//check
//	if md5 == "" {
//		return errors.New("invalid parameter")
//	}
//	//remove or update element with locker
//	f.Lock()
//	defer f.Unlock()
//	removeIdx := -1
//	for idx, v := range f.removedFiles {
//		if v.md5 == md5 {
//			removeIdx = idx
//			break
//		}
//	}
//	if removeIdx >= 0 {
//		//remove relate element
//		f.removedFiles = append(f.removedFiles[0:removeIdx], f.removedFiles[removeIdx:]...)
//
//		//remove from storage
//		err = f.delFileBase(md5)
//	}
//
//	//check and reset slice
//	if len(f.removedFiles) <= 0 {
//		newFileSlice := make([]*removedBaseFile, 0)
//		f.removedFiles = newFileSlice
//
//		//gc opt
//		runtime.GC()
//	}
//	return err
//}

//get available removed file base info
func (f *Chunk) GetAvailableRemovedFileBase(
	dataSize int64) (*json.FileBaseJson, error) {
	var (
		matchedMd5 string
		err error
	)
	//check
	if dataSize <= 0 {
		return nil, errors.New("invalid parameter")
	}

	//get valid removed file base md5
	matchedMd5, err = f.removed.GetAvailableRemovedFileBase(dataSize)
	if matchedMd5 == "" {
		return nil, err
	}

	//get file base info
	fileBase, subErr := f.getFileBase(matchedMd5)
	return fileBase, subErr
}

//add new removed file base info
func (f *Chunk) AddRemovedBaseInfo(
	obj *json.FileBaseJson) error {
	//check
	if obj == nil {
		return errors.New("invalid parameter")
	}

	//add into removed file base
	err := f.removed.AddRemoved(obj.Md5, obj.Blocks)
	if err != nil {
		return err
	}

	//force save removed data
	err = f.removed.SaveRemoved()
	return err
}

//save removed
func (f *Chunk) SaveRemoved() error {
	return f.removed.SaveRemoved()
}

//set redis
func (f *Chunk) SetUseRedis(useRedis bool) {
	f.useRedis = useRedis
	f.SetBaseUseRedis(useRedis)
}

//set inter data
func (f *Chunk) SetData(data *data.InterRedisData) {
	f.data = data
}

//set config
func (f *Chunk) SetConfig(cfg *conf.Config) {
	if cfg == nil {
		return
	}
	f.cfg = cfg

	//set removed config
	f.removed.SetConfig(cfg)
}