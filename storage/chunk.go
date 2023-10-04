package storage

import (
	"errors"
	"github.com/andyzhou/pond/define"
	"github.com/andyzhou/pond/json"
	"github.com/andyzhou/pond/search"
	"sync"
)

/*
 * chunk base face
 * - mark removed and re-use logic
 */

//face info
type Chunk struct {
	removedFiles []*json.FileBaseJson
	sync.RWMutex
}

//construct
func NewChunk() *Chunk {
	this := &Chunk{
		removedFiles: []*json.FileBaseJson{},
	}
	return this
}

//remove file base info
func (f *Chunk) RemoveRemovedFileBase(md5 string) error {
	//check
	if md5 == "" {
		return errors.New("invalid parameter")
	}
	//remove or update element with locker
	f.Lock()
	defer f.Unlock()
	removeIdx := -1
	for idx, v := range f.removedFiles {
		if v.Md5 == md5 {
			removeIdx = idx
			break
		}
	}
	if removeIdx >= 0 {
		//remove relate element
		f.removedFiles = append(f.removedFiles[0:removeIdx], f.removedFiles[removeIdx:]...)
	}
	if len(f.removedFiles) <= 0 {
		newFileSlice := make([]*json.FileBaseJson, 0)
		f.removedFiles = newFileSlice
	}
	return nil
}

//update removed file base info
func (f *Chunk) UpdateRemovedFileBase(
		md5 string,
		usedSize int64) error {
	//check
	if md5 == "" || usedSize <= 0 {
		return errors.New("invalid parameter")
	}

	//get origin file base by md5
	fileBaseSearch := search.GetSearch().GetFileBase()
	fileBaseObj, err := fileBaseSearch.GetOne(md5)
	if err != nil {
		return err
	}
	if fileBaseObj == nil || !fileBaseObj.Removed {
		return errors.New("invalid file md5")
	}

	//update file base obj
	isRemoveOpt := false
	leftBlocks := fileBaseObj.Blocks - usedSize
	if leftBlocks < define.DefaultChunkBlockSize {
		//no any free blocks, removed it.
		err = fileBaseSearch.DelOne(md5)
		isRemoveOpt = true
	}else{
		//update file base obj
		fileBaseObj.Offset += usedSize
		fileBaseObj.Blocks -= usedSize
		err = fileBaseSearch.AddOne(fileBaseObj)
	}
	if err != nil {
		return err
	}

	//remove or update element with locker
	f.Lock()
	defer f.Unlock()
	removeIdx := -1
	for idx, v := range f.removedFiles {
		if isRemoveOpt {
			removeIdx = idx
			break
		}else{
			//update element
			*v = *fileBaseObj
		}
	}
	if removeIdx >= 0 {
		//remove relate element
		f.removedFiles = append(f.removedFiles[0:removeIdx], f.removedFiles[removeIdx:]...)
	}
	return nil
}

//get available removed file base info
func (f *Chunk) GetAvailableRemovedFileBase(
			dataSize int64,
		) (*json.FileBaseJson, error) {
	//check
	if dataSize <= 0 {
		return nil, errors.New("invalid parameter")
	}
	if f.removedFiles == nil || len(f.removedFiles) <= 0 {
		return nil, nil
	}

	//pick matched obj
	tryTimes := 1
	for {
		found := false
		//setup data max size
		dataSizeMaxBase := int64(float64(dataSize) * (1 + float64(tryTimes) * define.DefaultChunkMultiIncr))
		for _, v := range f.removedFiles {
			if v.Blocks >= dataSize && v.Blocks < dataSizeMaxBase {
				return v, nil
			}
		}
		if !found {
			tryTimes++
		}
		if found || tryTimes >= define.ChunkMultiTryTimes {
			//force break
			break
		}
	}
	return nil, nil
}

//add new removed file base info
func (f *Chunk) AddRemovedBaseInfo(obj *json.FileBaseJson) error {
	//check
	if obj == nil {
		return errors.New("invalid parameter")
	}
	f.Lock()
	defer f.Unlock()
	f.removedFiles = append(f.removedFiles, obj)
	return nil
}

//load data
func (f *Chunk) Load() {
	f.loadRemovedFiles()
}

////////////////
//private func
////////////////

//load removed base file info
func (f *Chunk) loadRemovedFiles() {
	//setup
	page := define.DefaultPage
	pageSize := define.DefaultPageSizeMax

	//load matched data from search
	fileBaseSearch := search.GetSearch().GetFileBase()
	for {
		//get batch records
		total, recSlice, err := fileBaseSearch.GetBatchByRemoved(page, pageSize)
		if total <= 0 || err != nil ||
			recSlice == nil || len(recSlice) <= 0 {
			break
		}
		f.removedFiles = append(f.removedFiles, recSlice...)
	}
}
