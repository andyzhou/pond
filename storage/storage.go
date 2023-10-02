package storage

import (
	"errors"
	"github.com/andyzhou/pond/conf"
	"github.com/andyzhou/pond/json"
	"github.com/andyzhou/pond/search"
	"github.com/andyzhou/pond/util"
	"net/http"
	"time"
)

/*
 * inter storage face
 * - include meta and chunk data
 * - write request in process
 */

//face info
type Storage struct {
	cfg *conf.Config //reference
	manager *Manager
	initDone bool
	util.Util
}

//construct
func NewStorage() *Storage {
	this := &Storage{
		manager: NewManager(),
	}
	return this
}

//quit
func (f *Storage) Quit() {
	f.manager.Quit()
}

//get file info list from search
func (f *Storage) GetFilesInfo(
			page, pageSize int,
		) (int64, []*json.FileInfoJson, error) {
	//check
	if !f.initDone {
		return 0, nil, errors.New("hasn't setup root path")
	}
	//search file info
	fileInfoSearch := search.GetSearch().GetFileInfo()
	return fileInfoSearch.GetBathByTime(page, pageSize)
}

//del real data
func (f *Storage) DelRealData(shortUrl string) error {
	//check
	if !f.initDone {
		return errors.New("hasn't setup root path")
	}
	if shortUrl == "" {
		return errors.New("invalid parameter")
	}

	//get relate search
	baseInfoSearch := search.GetSearch().GetFileBase()
	fileInfoSearch := search.GetSearch().GetFileInfo()

	//get file info
	fileInfo, err := fileInfoSearch.GetOne(shortUrl)
	if err != nil || fileInfo == nil {
		return err
	}

	//get file base info
	baseInfo, subErr := baseInfoSearch.GetOne(fileInfo.Md5)
	if subErr != nil || baseInfo == nil {
		return subErr
	}
	if baseInfo.Removed {
		return errors.New("file has removed")
	}

	//mark base info status
	baseInfo.Removed = true
	err = baseInfoSearch.AddOne(baseInfo)
	return err
}

//delete data
//just remove file info from search
func (f *Storage) DeleteData(shortUrl string) error {
	//check
	if !f.initDone {
		return errors.New("hasn't setup root path")
	}
	if shortUrl == "" {
		return errors.New("invalid parameter")
	}

	//del file info
	fileInfoSearch := search.GetSearch().GetFileInfo()
	err := fileInfoSearch.DelOne(shortUrl)
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
	if !f.initDone {
		return nil, errors.New("hasn't setup root path")
	}
	if shortUrl == "" {
		return nil, errors.New("invalid parameter")
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

//write data
//return shortUrl, error
func (f *Storage) WriteData(data []byte) (string, error) {
	var (
		shortUrl string
	)
	//check
	if !f.initDone {
		return shortUrl, errors.New("hasn't setup root path")
	}
	if data == nil {
		return shortUrl, errors.New("invalid parameter")
	}

	//gen and check base file by md5
	md5Val, err := f.Md5Sum(data)
	if err != nil {
		return shortUrl, err
	}

	//pick active chunk
	activeChunk, err := f.manager.GetActiveChunk()
	if err != nil {
		return shortUrl, err
	}

	//get relate search
	fileInfoSearch := search.GetSearch().GetFileInfo()
	fileBaseSearch := search.GetSearch().GetFileBase()

	//check file base info
	fileBaseObj, _ := fileBaseSearch.GetOne(md5Val)
	if fileBaseObj == nil {
		//write file base byte data
		resp := activeChunk.WriteFile(data)
		if resp.Err != nil {
			return shortUrl, err
		}

		//create new file base info
		fileBaseObj = json.NewFileBaseJson()
		fileBaseObj.Md5 = md5Val
		fileBaseObj.ChunkFileId = activeChunk.GetFileId()
		fileBaseObj.Size = int64(len(data))
		fileBaseObj.Offset = resp.NewOffSet
		fileBaseObj.Blocks = resp.BlockSize
		fileBaseObj.CreateAt = time.Now().Unix()

		//save into search
		subErr := fileBaseSearch.AddOne(fileBaseObj)
		if subErr != nil {
			return shortUrl, subErr
		}
	}

	//gen new data short url
	shortUrl, err = f.manager.GenNewShortUrl()
	if err != nil {
		return shortUrl, err
	}

	//create new file info
	fileInfoObj := json.NewFileInfoJson()
	fileInfoObj.ShortUrl = shortUrl
	fileInfoObj.Md5 = md5Val
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

//set config
func (f *Storage) SetConfig(cfg *conf.Config) error {
	//check
	if cfg == nil || cfg.DataPath == "" {
		return errors.New("invalid parameter")
	}
	//search setup
	err := search.GetSearch().SetRootPath(cfg.DataPath)
	if err != nil {
		return err
	}

	//manager setup
	f.cfg = cfg
	err = f.manager.SetConfig(cfg)
	if err != nil {
		return err
	}
	f.initDone = true
	return nil
}