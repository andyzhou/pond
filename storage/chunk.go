package storage

import (
	"errors"
	"sort"
	"sync"

	"github.com/andyzhou/pond/data"
	"github.com/andyzhou/pond/define"
	"github.com/andyzhou/pond/json"
	"github.com/andyzhou/pond/search"
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
	removedFiles removedBaseFileSort
	Base
	sync.RWMutex
}

//construct
func NewChunk() *Chunk {
	this := &Chunk{
		removedFiles: []*removedBaseFile{},
	}
	return this
}

//remove file base info
func (f *Chunk) RemoveRemovedFileBase(md5 string) error {
	var (
		err error
	)
	//check
	if md5 == "" {
		return errors.New("invalid parameter")
	}
	//remove or update element with locker
	f.Lock()
	defer f.Unlock()
	removeIdx := -1
	for idx, v := range f.removedFiles {
		if v.md5 == md5 {
			removeIdx = idx
			break
		}
	}
	if removeIdx >= 0 {
		//remove relate element
		f.removedFiles = append(f.removedFiles[0:removeIdx], f.removedFiles[removeIdx:]...)

		//remove from storage
		err = f.delFileBase(md5)
	}

	//check and reset slice
	if len(f.removedFiles) <= 0 {
		newFileSlice := make([]*removedBaseFile, 0)
		f.removedFiles = newFileSlice
	}
	return err
}

////update removed file base info
//func (f *Chunk) UpdateRemovedFileBase(
//		md5 string,
//		usedSize int64) error {
//	//check
//	if md5 == "" || usedSize <= 0 {
//		return errors.New("invalid parameter")
//	}
//
//	//get origin file base by md5
//	fileBaseSearch := search.GetSearch().GetFileBase()
//	fileBaseObj, err := fileBaseSearch.GetOne(md5)
//	if err != nil {
//		return err
//	}
//	if fileBaseObj == nil || !fileBaseObj.Removed {
//		return errors.New("invalid file md5")
//	}
//
//	//update file base obj
//	isRemoveOpt := false
//	leftBlocks := fileBaseObj.Blocks - usedSize
//	if leftBlocks < define.DefaultChunkBlockSize {
//		//no any free blocks, removed it.
//		err = fileBaseSearch.DelOne(md5)
//		isRemoveOpt = true
//	}else{
//		//update file base obj
//		fileBaseObj.Offset += usedSize
//		fileBaseObj.Blocks -= usedSize
//		err = fileBaseSearch.AddOne(fileBaseObj)
//	}
//	if err != nil {
//		return err
//	}
//
//	//remove or update element with locker
//	f.Lock()
//	defer f.Unlock()
//	removeIdx := -1
//	for idx, v := range f.removedFiles {
//		if isRemoveOpt {
//			removeIdx = idx
//			break
//		}else{
//			//update element
//			*v = *fileBaseObj
//		}
//	}
//	if removeIdx >= 0 {
//		//remove relate element
//		f.removedFiles = append(f.removedFiles[0:removeIdx], f.removedFiles[removeIdx:]...)
//	}
//	return nil
//}

//get available removed file base info
func (f *Chunk) GetAvailableRemovedFileBase(
	dataSize int64) (*json.FileBaseJson, error) {
	var (
		matchedMd5 string
	)
	//check
	if dataSize <= 0 {
		return nil, errors.New("invalid parameter")
	}
	if f.removedFiles == nil || len(f.removedFiles) <= 0 {
		return nil, nil
	}

	//pick matched obj with locker
	f.Lock()
	defer f.Unlock()
	tryTimes := 1
	for {
		found := false
		//setup data max size
		dataSizeMaxBase := int64(float64(dataSize) * (1 + float64(tryTimes) * define.DefaultChunkMultiIncr))
		for _, v := range f.removedFiles {
			if v.blocks >= dataSize && v.blocks < dataSizeMaxBase {
				matchedMd5 = v.md5
				found = true
				break
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
	if matchedMd5 == "" {
		return nil, nil
	}

	//get file base info
	fileBase, err := f.getFileBase(matchedMd5)
	return fileBase, err
}

//add new removed file base info
func (f *Chunk) AddRemovedBaseInfo(
	obj *json.FileBaseJson) error {
	//check
	if obj == nil {
		return errors.New("invalid parameter")
	}

	//add base info with locker
	f.Lock()
	defer f.Unlock()
	foundIdx := -1
	for idx, v := range f.removedFiles {
		if v.md5 == obj.Md5 {
			//found
			foundIdx = idx
			break
		}
	}
	if foundIdx < 0 {
		//not found, add new
		info := &removedBaseFile{
			md5: obj.Md5,
			blocks: obj.Blocks,
		}
		f.removedFiles = append(f.removedFiles, info)

		//sorter
		sort.Sort(f.removedFiles)

		//save new removed file base
		f.addNewRemovedFile(obj)
	}
	return nil
}

//load data
//run in son process
func (f *Chunk) Load() {
	go f.loadRemovedFiles()
}

func (f *Chunk) SetUseRedis(useRedis bool) {
	f.useRedis = useRedis
	f.SetBaseUseRedis(useRedis)
}

func (f *Chunk) SetData(data *data.InterRedisData) {
	f.data = data
}

////////////////
//private func
////////////////\

//add new removed base file info
func (f *Chunk) addNewRemovedFile(obj *json.FileBaseJson) error {
	var (
		err error
	)
	//check
	if obj == nil {
		return errors.New("invalid parameter")
	}

	if f.useRedis {
		//save into redis
		fileData := f.data.GetFile()
		err = fileData.AddRemovedFileBase(obj.Md5, obj.Blocks)
	}else{
		//save int search
		fileBaseSearch := search.GetSearch().GetFileBase()
		err = fileBaseSearch.AddOne(obj)
	}
	return err
}

//load removed base file info
func (f *Chunk) loadRemovedFiles() {
	//setup
	page := define.DefaultPage
	pageSize := define.DefaultPageSizeMax

	if f.useRedis {
		//load from redis
		fileData := f.data.GetFile()
		for {
			//get batch records
			zSlice, err := fileData.LoadRemovedFileBase(page, pageSize)
			if err != nil || zSlice == nil || len(zSlice) <= 0 {
				break
			}
			for _, v := range zSlice {
				md5, _ := v.Member.(string)
				blocks := int64(v.Score)
				if md5 == "" || blocks <= 0 {
					continue
				}
				obj := &removedBaseFile{
					md5: md5,
					blocks: blocks,
				}
				f.removedFiles = append(f.removedFiles, obj)
			}
		}
	}else{
		//load from search
		//load matched data from search
		fileBaseSearch := search.GetSearch().GetFileBase()

		//add data locker
		f.Lock()
		defer f.Unlock()
		for {
			//get batch records
			total, recSlice, err := fileBaseSearch.GetBatchByRemoved(page, pageSize)
			if total <= 0 || err != nil ||
				recSlice == nil || len(recSlice) <= 0 {
				break
			}
			for _, v := range recSlice {
				obj := &removedBaseFile{
					md5: v.Md5,
					blocks: v.Blocks,
				}
				f.removedFiles = append(f.removedFiles, obj)
			}
			page++
		}
	}
}
