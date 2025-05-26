package storage

import (
	"errors"
	"fmt"
	"math/rand"
	"net/http"
	"sync"
	"time"

	"github.com/andyzhou/pond/chunk"
	"github.com/andyzhou/pond/conf"
	"github.com/andyzhou/pond/data"
	"github.com/andyzhou/pond/define"
	"github.com/andyzhou/pond/json"
	"github.com/andyzhou/pond/search"
	"github.com/andyzhou/pond/utils"
)

/*
 * inter storage face
 * @author <AndyZhou>
 * @mail <diudiu8848@163.com>
 * - include meta and chunk data
 * - write/del request in queues
 */

//face info
type Storage struct {
	wg           *sync.WaitGroup   //reference
	cfg          *conf.Config      //reference
	redisCfg     *conf.RedisConfig //reference
	manager      *Manager
	data         *data.InterRedisData
	useRedis     bool
	initDone     bool
	searchLocker sync.RWMutex
	Base
	utils.Utils
}

//construct
func NewStorage(wg *sync.WaitGroup) *Storage {
	this := &Storage{
		wg: wg,
		manager: NewManager(wg),
		data: data.NewInterRedisData(),
	}
	return this
}

//quit
func (f *Storage) Quit() {
	f.manager.Quit()
	if !f.useRedis {
		search.GetSearch().Quit()
	}
}

//get all chunk active size
func (f *Storage) GetAllChunkActiveSize() map[int64]int64 {
	return f.manager.GetAllChunkSize()
}

//get file info list from search
//sync opt
func (f *Storage) GetFilesInfo(
		page, pageSize int,
	) (int64, []*json.FileInfoJson, error) {
	//check
	if !f.initDone {
		return 0, nil, errors.New("config didn't setup")
	}
	if f.useRedis {
		//get from redis
		filesInfo, err := f.data.GetFile().GetInfoList(page, pageSize)
		return 0, filesInfo, err
	}else{
		//get from search file info
		fileInfoSearch := search.GetSearch().GetFileInfo()
		return fileInfoSearch.GetBathByTime(page, pageSize)
	}
}

//delete data
//just remove file info from search
func (f *Storage) DeleteData(
	shortUrl string) error {
	//check
	if shortUrl == "" {
		return errors.New("invalid parameter")
	}
	if !f.initDone {
		return errors.New("config didn't setup")
	}

	//get file info
	fileInfo, _ := f.getFileInfo(shortUrl)
	if fileInfo == nil {
		return errors.New("can't get file info by short url")
	}

	//get file base
	fileBase, _ := f.getFileBase(fileInfo.Md5)
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
	err := f.saveFileBase(fileBase)
	if err != nil {
		return err
	}

	//del file info
	err = f.delFileInfo(shortUrl)

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
		offsetAndEnds ...int64,
	) ([]byte, error) {
	var (
		assignedOffset, assignedEnd int64
		realEnd int64
	)
	//check
	if shortUrl == "" {
		return nil, errors.New("invalid parameter")
	}
	if !f.initDone {
		return nil, errors.New("config didn't setup")
	}

	//get file info
	fileInfo, err := f.getFileInfo(shortUrl)
	if err != nil {
		return nil, err
	}
	if fileInfo == nil {
		return nil, errors.New("can't get file info")
	}

	//get relate chunk data
	chunkObj, subErr := f.manager.GetChunkById(fileInfo.ChunkFileId)
	if subErr != nil || chunkObj == nil {
		return nil, subErr
	}

	//detect assigned offset and length
	if offsetAndEnds != nil {
		paraLen := len(offsetAndEnds)
		switch paraLen {
		case 1:
			{
				assignedOffset = offsetAndEnds[0]
			}
		case 2:
			{
				assignedOffset = offsetAndEnds[0]
				assignedEnd = offsetAndEnds[1]
			}
		}
	}

	//setup real offset and length
	realOffset := fileInfo.Offset
	skipHeader := false
	if assignedOffset >= 0 {
		realOffset += assignedOffset
		skipHeader = true
	}
	if assignedEnd > 0 {
		if assignedEnd < fileInfo.Size {
			realEnd = realOffset + assignedEnd
		}else{
			realEnd = fileInfo.Offset + fileInfo.Size
		}
	}else{
		realEnd = realOffset + fileInfo.Size
	}

	//read chunk file data
	fileData, subErrTwo := chunkObj.ReadFile(realOffset, realEnd, skipHeader)
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
	redisCfg ...*conf.RedisConfig) error {
	var (
		oneRedisCfg *conf.RedisConfig
	)
	//check
	if cfg == nil || cfg.DataPath == "" {
		return errors.New("invalid parameter")
	}

	//init redis data
	if redisCfg != nil && len(redisCfg) > 0 {
		oneRedisCfg = redisCfg[0]
		if oneRedisCfg != nil {
			if oneRedisCfg.GroupTag == "" {
				oneRedisCfg.GroupTag = define.DefaultRedisGroup
			}
		}
		f.setRedisConfig(oneRedisCfg)
		f.SetBaseUseRedis(true)
		f.SetBaseData(f.data)
		f.manager.SetData(f.data)
	}else{
		//search setup
		err := search.GetSearch().SetCore(cfg.DataPath, cfg.InterQueueSize)
		if err != nil {
			return err
		}
	}

	//manager setup
	f.cfg = cfg
	err := f.manager.SetConfig(cfg, f.useRedis)
	f.initDone = true
	return err
}

///////////////
//private func
///////////////

//set redis config
func (f *Storage) setRedisConfig(cfg *conf.RedisConfig) error {
	if cfg == nil {
		return errors.New("invalid parameter")
	}
	f.redisCfg = cfg
	f.useRedis = true
	f.data.SetRedisConf(cfg)
	return nil
}

//overwrite old data
//fix chunk size config should be true
func (f *Storage) overwriteData(shortUrl string, fileData[]byte) error {
	var (
		fileInfoObj *json.FileInfoJson
		fileBaseObj *json.FileBaseJson
		err error
	)
	//check
	if shortUrl == "" || fileData == nil {
		return errors.New("invalid parameter")
	}

	//get file and base info
	if f.useRedis {
		//use redis data
		fileInfoData := f.data.GetFile()
		fileInfoObj, err = fileInfoData.GetInfo(shortUrl)
		if err != nil || fileInfoObj == nil {
			return errors.New("no file info for this short url")
		}
		fileBaseObj, err = fileInfoData.GetBase(fileInfoObj.Md5)
		if err != nil || fileBaseObj == nil {
			return errors.New("no file base info for this short url")
		}
	}else{
		//use search data
		fileInfoSearch := search.GetSearch().GetFileInfo()
		fileBaseSearch := search.GetSearch().GetFileBase()
		fileInfoObj, err = fileInfoSearch.GetOne(shortUrl)
		if err != nil || fileInfoObj == nil {
			return errors.New("no file info for this short url")
		}
		fileBaseObj, err = fileBaseSearch.GetOne(fileInfoObj.Md5)
		if err != nil || fileBaseObj == nil {
			return errors.New("can't get file base info")
		}
	}

	dataLen := int64(len(fileData))
	fileMd5 := fileInfoObj.Md5
	offset := fileInfoObj.Offset
	if fileInfoObj.Blocks < dataLen {
		return errors.New("new file data size exceed old data")
	}

	//get assigned chunk
	activeChunk, subErr := f.manager.GetChunkById(fileInfoObj.ChunkFileId)
	if subErr != nil {
		return subErr
	}
	if activeChunk == nil {
		return errors.New("can't get active chunk")
	}

	//overwrite chunk data
	resp := activeChunk.WriteFile(fileMd5, fileData, offset)
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


	//update file info
	fileInfoObj.Size = dataLen
	fileInfoObj.Offset = resp.NewOffSet

	//save info and base data
	if f.useRedis {
		//save into redis
		fileInfoData := f.data.GetFile()
		err = fileInfoData.AddBase(fileBaseObj)
		err = fileInfoData.AddInfo(fileInfoObj)
	}else{
		//save into search
		fileInfoSearch := search.GetSearch().GetFileInfo()
		fileBaseSearch := search.GetSearch().GetFileBase()
		err = fileBaseSearch.AddOne(fileBaseObj)
		err = fileInfoSearch.AddOne(fileInfoObj)
	}
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
		offset int64 = -1
		err error
	)
	//check
	if data == nil || len(data) <= 0 {
		return shortUrl, errors.New("invalid parameter")
	}

	//gen and check base file by md5
	if f.cfg.CheckSame {
		//check same data, use data as md5 base value
		fileMd5, err = f.Md5Sum(data)
	}else{
		//not check same data
		//use rand num + time stamp as md5 base value
		now := time.Now().UnixNano()
		rand.Seed(now)
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
		fileBaseObj, _ = f.getFileBase(fileMd5)
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

			//get active chunk by file id
			activeChunk, err = f.manager.GetChunkById(fileBaseObj.ChunkFileId)

			//save removed
			f.manager.GetRunningChunk().SaveRemoved()
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
		resp := activeChunk.WriteFile(fileBaseObj.Md5, data, offset)
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

	//save file base
	err = f.saveFileBase(fileBaseObj)
	if err != nil {
		return shortUrl, err
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

	//save file info
	err = f.saveFileInfo(fileInfoObj)
	return shortUrl, err
}