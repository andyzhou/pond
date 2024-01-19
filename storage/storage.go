package storage

import (
	"errors"
	"fmt"
	"github.com/andyzhou/pond/chunk"
	"github.com/andyzhou/pond/conf"
	"github.com/andyzhou/pond/define"
	"github.com/andyzhou/pond/json"
	"github.com/andyzhou/pond/search"
	"github.com/andyzhou/pond/utils"
	"math/rand"
	"net/http"
	"sync"
	"time"
)

/*
 * inter storage face
 * - include meta and chunk data
 * - write/del request in queues
 */

//face info
type Storage struct {
	wg *sync.WaitGroup //reference
	cfg *conf.Config //reference
	manager *Manager
	initDone bool
	searchLocker sync.RWMutex
	utils.Utils
}

//construct
func NewStorage(wg *sync.WaitGroup) *Storage {
	this := &Storage{
		manager: NewManager(wg),
	}
	return this
}

//quit
func (f *Storage) Quit() {
	f.manager.Quit()
	search.GetSearch().Quit()
}

//get file info list from search
func (f *Storage) GetFilesInfo(
			page, pageSize int,
		) (int64, []*json.FileInfoJson, error) {
	//check
	if !f.initDone {
		return 0, nil, errors.New("config didn't setup")
	}
	//search file info
	fileInfoSearch := search.GetSearch().GetFileInfo()
	return fileInfoSearch.GetBathByTime(page, pageSize)
}

//delete data
//just remove file info from search
func (f *Storage) DeleteData(shortUrl string) error {
	//check
	if shortUrl == "" {
		return errors.New("invalid parameter")
	}
	if !f.initDone {
		return errors.New("config didn't setup")
	}

	//get relate search
	fileInfoSearch := search.GetSearch().GetFileInfo()
	fileBaseSearch := search.GetSearch().GetFileBase()

	//get file info
	fileInfo, _ := fileInfoSearch.GetOne(shortUrl)
	if fileInfo == nil {
		return errors.New("can't get file info by short url")
	}

	//get file base
	fileBase, _ := fileBaseSearch.GetOne(fileInfo.Md5)
	if fileBase == nil {
		return errors.New("can't get file base info")
	}

	//decr appoint value
	fileBase.Appoints--
	if fileBase.Appoints <= 0 {
		//update removed status
		fileBase.Removed = true
		fileBase.Appoints = 0
	}

	//update file base info
	err := fileBaseSearch.AddOne(fileBase)
	if err != nil {
		return err
	}

	//del file info
	err = fileInfoSearch.DelOne(shortUrl)

	//add removed info into run env
	if fileBase.Removed && err == nil {
		f.manager.GetRunningChunk().AddRemovedBaseInfo(fileBase)
	}
	return err
}

//read data
//extend para: offset, length
//return fileData, error
func (f *Storage) ReadData(
			shortUrl string,
			offsetAndLength ...int64,
		) ([]byte, error) {
	var (
		assignedOffset, assignedLength int64
	)
	//check
	if shortUrl == "" {
		return nil, errors.New("invalid parameter")
	}
	if !f.initDone {
		return nil, errors.New("config didn't setup")
	}

	//get file info
	fileInfoSearch := search.GetSearch().GetFileInfo()
	fileInfo, err := fileInfoSearch.GetOne(shortUrl)
	if err != nil || fileInfo == nil {
		return nil, err
	}

	//get relate chunk data
	chunkObj, subErr := f.manager.GetChunkById(fileInfo.ChunkFileId)
	if subErr != nil || chunkObj == nil {
		return nil, subErr
	}

	//detect assigned offset and length
	if offsetAndLength != nil {
		paraLen := len(offsetAndLength)
		switch paraLen {
		case 1:
			{
				assignedOffset = offsetAndLength[0]
			}
		case 2:
			{
				assignedOffset = offsetAndLength[0]
				assignedLength = offsetAndLength[1]
			}
		}
	}

	//setup real offset and length
	realOffset := fileInfo.Offset
	realLength := fileInfo.Size
	if assignedOffset >= 0 && assignedOffset <= (fileInfo.Offset + fileInfo.Size) {
		realOffset = fileInfo.Offset + assignedOffset
	}
	if assignedLength > 0 && assignedLength <= fileInfo.Size {
		realLength = assignedLength
	}

	//read chunk file data
	fileData, subErrTwo := chunkObj.ReadFile(realOffset, realLength)
	return fileData, subErrTwo
}

//write new or old data
//if assigned short url means overwrite old data
//if overwrite data, fix chunk size config should be true
//return shortUrl, error
func (f *Storage) WriteData(
			data []byte,
			shortUrls ...string,
		) (string, error) {
	var (
		shortUrl string
		err error
	)
	//check
	if data == nil || len(data) <= 0 {
		return shortUrl, errors.New("invalid parameter")
	}
	if !f.initDone {
		return shortUrl, errors.New("config didn't setup")
	}

	//detect
	if shortUrls != nil && len(shortUrls) > 0 {
		shortUrl = shortUrls[0]
	}

	//overwrite data should setup fix chunk size config
	if shortUrl != "" && !f.cfg.FixedBlockSize {
		return shortUrl, errors.New("config need set fix chunk size as true")
	}
	if shortUrl != "" {
		//over write data
		err = f.overwriteData(shortUrl, data)
	}else{
		//write new data
		shortUrl, err = f.writeNewData(data)
	}
	return shortUrl, err
}

//set config
func (f *Storage) SetConfig(
	cfg *conf.Config,
	wg *sync.WaitGroup) error {
	//check
	if cfg == nil || cfg.DataPath == "" || wg == nil {
		return errors.New("invalid parameter")
	}
	f.wg = wg

	//search setup
	err := search.GetSearch().SetCore(cfg.DataPath, wg)
	if err != nil {
		return err
	}

	//manager setup
	f.cfg = cfg
	err = f.manager.SetConfig(cfg)
	f.initDone = true
	return nil
}

///////////////
//private func
///////////////

//overwrite old data
//fix chunk size config should be true
func (f *Storage) overwriteData(shortUrl string, data[]byte) error {
	//check
	if shortUrl == "" || data == nil {
		return errors.New("invalid parameter")
	}

	//get relate search
	fileInfoSearch := search.GetSearch().GetFileInfo()
	fileBaseSearch := search.GetSearch().GetFileBase()

	//get file info
	fileInfoObj, _ := fileInfoSearch.GetOne(shortUrl)
	if fileInfoObj == nil {
		return errors.New("no file info for this short url")
	}

	dataLen := int64(len(data))
	fileMd5 := fileInfoObj.Md5
	offset := fileInfoObj.Offset
	if fileInfoObj.Blocks < dataLen {
		return errors.New("new file data size exceed old data")
	}

	//get file base info
	fileBaseObj, _ := fileBaseSearch.GetOne(fileMd5)
	if fileBaseObj == nil {
		return errors.New("can't get file base info")
	}

	//get assigned chunk
	activeChunk, err := f.manager.GetChunkById(fileInfoObj.ChunkFileId)
	if err != nil {
		return err
	}
	if activeChunk == nil {
		return errors.New("can't get active chunk")
	}

	//overwrite chunk data
	resp := activeChunk.WriteFile(data, offset)
	if resp == nil {
		return errors.New("can't get chunk write file response")
	}
	if resp.Err != nil {
		return resp.Err
	}

	//update file base with locker
	f.searchLocker.Lock()
	defer f.searchLocker.Unlock()

	fileBaseObj.Size = dataLen
	fileBaseObj.Blocks = resp.BlockSize
	fileBaseSearch.AddOne(fileBaseObj)

	//update file info
	fileInfoObj.Size = dataLen
	fileInfoObj.Offset = resp.NewOffSet
	err = fileInfoSearch.AddOne(fileInfoObj)
	return err
}

//write new data
//support removed data re-use
//use locker for atomic opt
func (f *Storage) writeNewData(data []byte) (string, error) {
	var (
		fileMd5 string
		shortUrl string
		fileBaseObj *json.FileBaseJson
		activeChunk *chunk.Chunk
		needRemovedMd5 string
		offset int64 = -1
		err error
	)
	//check
	if data == nil || len(data) <= 0 {
		return shortUrl, errors.New("invalid parameter")
	}

	//get relate search
	fileInfoSearch := search.GetSearch().GetFileInfo()
	fileBaseSearch := search.GetSearch().GetFileBase()

	//gen and check base file by md5
	if f.cfg.CheckSame {
		//check same data, use data as md5 base value
		fileMd5, err = f.Md5Sum(data)
	}else{
		//not check same data
		//use rand num + time stamp as md5 base value
		now := time.Now().UnixNano()
		randInt := rand.Int63n(now)
		md5ValBase := fmt.Sprintf("%v:%v", randInt, now)
		fileMd5, err = f.Md5Sum([]byte(md5ValBase))
	}
	if err != nil || fileMd5 == "" {
		return shortUrl, err
	}

	dataSize := int64(len(data))
	needWriteChunkData := true
	if f.cfg.CheckSame {
		//need check same, check file base info
		fileBaseObj, _ = fileBaseSearch.GetOne(fileMd5)
		if fileBaseObj != nil {
			if !fileBaseObj.Removed {
				//inc appoint value of file base info
				fileBaseObj.Appoints++
			}
			needWriteChunkData = false
		}
	}

	if needWriteChunkData {
		//get removed chunk block data
		removedFileBase, _ := f.manager.GetRunningChunk().GetAvailableRemovedFileBase(dataSize)
		if removedFileBase != nil {
			//set file base obj
			fileBaseObj = removedFileBase
			fileBaseObj.Size = dataSize
			fileBaseObj.Removed = false
			fileBaseObj.Appoints = define.DefaultFileAppoint

			//others setup
			offset = fileBaseObj.Offset
			needRemovedMd5 = removedFileBase.Md5

			//get active chunk by file id
			activeChunk, err = f.manager.GetChunkById(fileBaseObj.ChunkFileId)
		}

		//check and pick active chunk
		if activeChunk == nil {
			activeChunk, err = f.manager.GetActiveChunk()
		}
		if err != nil {
			return shortUrl, err
		}
		if activeChunk == nil {
			return shortUrl, errors.New("can't get active chunk")
		}
	}

	//check or update file base info
	if fileBaseObj == nil {
		//create new file base info
		fileBaseObj = json.NewFileBaseJson()
		fileBaseObj.Md5 = fileMd5
		fileBaseObj.ChunkFileId = activeChunk.GetFileId()
		fileBaseObj.Size = dataSize
		fileBaseObj.Appoints = define.DefaultFileAppoint
		fileBaseObj.CreateAt = time.Now().Unix()
	}

	if needWriteChunkData && activeChunk != nil {
		//write file base byte data
		resp := activeChunk.WriteFile(data, offset)
		if resp == nil {
			return shortUrl, errors.New("can't get chunk write file response")
		}
		if resp.Err != nil {
			return shortUrl, resp.Err
		}
		//update file base
		fileBaseObj.Offset = resp.NewOffSet
		fileBaseObj.Blocks = resp.BlockSize
	}

	//gen new data short url
	shortUrl, err = f.manager.GenNewShortUrl()
	if err != nil {
		return shortUrl, err
	}

	//save into search with locker
	f.searchLocker.Lock()
	defer f.searchLocker.Unlock()
	err = fileBaseSearch.AddOne(fileBaseObj)
	if err != nil {
		return shortUrl, err
	}

	if needRemovedMd5 != "" {
		//remove running removed file base info
		f.manager.GetRunningChunk().RemoveRemovedFileBase(needRemovedMd5)
	}

	//create new file info
	fileInfoObj := json.NewFileInfoJson()
	fileInfoObj.ShortUrl = shortUrl
	fileInfoObj.Md5 = fileMd5
	fileInfoObj.ContentType = http.DetectContentType(data)
	fileInfoObj.Size = int64(len(data))
	fileInfoObj.ChunkFileId = fileBaseObj.ChunkFileId
	fileInfoObj.Offset = fileBaseObj.Offset
	fileInfoObj.Blocks = fileBaseObj.Blocks
	fileInfoObj.CreateAt = time.Now().Unix()

	//save into search
	err = fileInfoSearch.AddOne(fileInfoObj)
	return shortUrl, err
}