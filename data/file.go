package data

import (
	"errors"
	"fmt"
	"time"

	"github.com/andyzhou/pond/conf"
	"github.com/andyzhou/pond/data/base"
	"github.com/andyzhou/pond/define"
	"github.com/andyzhou/pond/json"
	"github.com/andyzhou/tinylib/util"
	genRedis "github.com/go-redis/redis/v8"
)

/*
 * @author <AndyZhou>
 * @mail <diudiu8848@163.com>
 * file base and info data
 * - use batch hash table keys
 * - hashed by the first two element of short url or md5
 */

//data face
type FileData struct {
	cfg *conf.RedisConfig
	hash *base.HashData
	sorted *base.SortedData
	initDone bool
	base.Base
	util.Util
}

//construct
func NewFileData() *FileData {
	this := &FileData{
	}
	return this
}

///////////////////
//api for removed
///////////////////

//load batch removed file base
func (f *FileData) LoadRemovedFileBase(
		page, pageSize int,
		isByDesc ...bool,
	) ([]genRedis.Z, error){
	if page <= 0 {
		page = define.DefaultPage
	}
	start := (page - 1) * pageSize
	end := start + pageSize
	zSlice, err := f.sorted.GetBatchMembers(
						f.getRemovedFileBaseKey(),
						start,
						end,
						isByDesc...)
	return zSlice, err
}

//remove removed file base
func (f *FileData) RemoveRemovedFileBase(md5 string) error {
	//check
	if md5 == "" {
		return errors.New("invalid parameter")
	}
	//remove member
	err := f.sorted.RemoveMember(f.getRemovedFileBaseKey(), md5)
	return err
}

//add removed file base
func (f *FileData) AddRemovedFileBase(md5 string, blocks int64) error {
	//check
	if md5 == "" || blocks <= 0 {
		return errors.New("invalid parameter")
	}
	//add member
	member := f.sorted.GenMember(md5, float64(blocks))
	err := f.sorted.AddMembers(f.getRemovedFileBaseKey(), member)
	return err
}

///////////////
//api for info
///////////////

//get file info list
func (f *FileData) GetInfoList(page, pageSize int) ([]*json.FileInfoJson, error) {
	//setup start, end value
	if page <= 0 {
		page = 1
	}
	if pageSize <= 0 {
		pageSize = define.DefaultPageSize
	}
	start := (page - 1) *pageSize
	end := start + pageSize

	//get from sort list
	listKeyTag := f.getFileListKey()
	zSlice, err := f.sorted.GetBatchMembers(listKeyTag, start, end, true)
	if err != nil || zSlice == nil || len(zSlice) <= 0 {
		return nil, err
	}

	//format result
	result := make([]*json.FileInfoJson, 0)
	for _, z := range zSlice {
		shortUrl, _ := z.Member.(string)
		if shortUrl == "" {
			continue
		}
		//get one file info
		fileInfo, _ := f.GetInfo(shortUrl)
		if fileInfo == nil || fileInfo.ShortUrl != shortUrl {
			continue
		}
		result = append(result, fileInfo)
	}
	return result, nil
}

//del file info
func (f *FileData) DelInfo(shortUrl string) error {
	//check
	if shortUrl == "" {
		return errors.New("invalid parameter")
	}

	//get key tag
	keyTag, err := f.getFileInfoKey(shortUrl)
	if err != nil {
		return err
	}

	//del from redis
	field := shortUrl
	err = f.hash.DelFields(keyTag, field)
	if err != nil {
		return err
	}

	//remove from file list
	listKeyTag := f.getFileListKey()
	err = f.sorted.RemoveMember(listKeyTag, shortUrl)
	return err
}

//get file info
func (f *FileData) GetInfo(shortUrl string) (*json.FileInfoJson, error) {
	//check
	if shortUrl == "" {
		return nil, errors.New("invalid parameter")
	}

	//get key tag
	keyTag, err := f.getFileInfoKey(shortUrl)
	if err != nil {
		return nil, err
	}

	//get from redis
	field := shortUrl
	jsonStr, subErr := f.hash.GetOneValue(keyTag, field)
	if subErr != nil || jsonStr == "" {
		return nil, subErr
	}

	//decode obj
	obj := json.NewFileInfoJson()
	err = obj.Decode([]byte(jsonStr), obj)
	return obj, err
}

//add file info
func (f *FileData) AddInfo(obj *json.FileInfoJson) error {
	//check
	if obj == nil || obj.ShortUrl == "" {
		return errors.New("invalid parameter")
	}

	//get key tag
	keyTag, err := f.getFileInfoKey(obj.ShortUrl)
	if err != nil {
		return err
	}

	//encode json string
	jsonStr, _ := obj.Encode2Str(obj)

	//save into redis
	field := obj.ShortUrl
	err = f.hash.SetOneValue(keyTag, field, jsonStr)
	if err != nil {
		return err
	}

	//add new file short url into sorted set
	//short url as member, time as score
	listKeyTag := f.getFileListKey()
	member := &genRedis.Z{
		Member: obj.ShortUrl,
		Score: float64(time.Now().Nanosecond()),
	}
	err = f.sorted.AddMembers(listKeyTag, member)
	return err
}

///////////////
//api for base
///////////////

//del file base info
func (f *FileData) DelBase(md5 string) error {
	//check
	if md5 == "" {
		return errors.New("invalid parameter")
	}

	//get key tag
	keyTag, err := f.getFileBaseKey(md5)
	if err != nil {
		return err
	}

	//del from redis
	field := md5
	err = f.hash.DelFields(keyTag, field)
	return err
}

//get file base info
func (f *FileData) GetBase(md5 string) (*json.FileBaseJson, error) {
	//check
	if md5 == "" {
		return nil, errors.New("invalid parameter")
	}

	//get key tag
	keyTag, err := f.getFileBaseKey(md5)
	if err != nil {
		return nil, err
	}

	//get from redis
	field := md5
	jsonStr, subErr := f.hash.GetOneValue(keyTag, field)
	if subErr != nil || jsonStr == "" {
		return nil, subErr
	}

	//decode obj
	obj := json.NewFileBaseJson()
	err = obj.Decode([]byte(jsonStr), obj)
	return obj, err
}

//add file base info
func (f *FileData) AddBase(obj *json.FileBaseJson) error {
	//check
	if obj == nil || obj.Md5 == "" {
		return errors.New("invalid parameter")
	}

	//get key tag
	keyTag, err := f.getFileBaseKey(obj.Md5)
	if err != nil {
		return err
	}

	//encode json string
	jsonStr, _ := obj.Encode2Str(obj)

	//save into redis
	field := obj.Md5
	err = f.hash.SetOneValue(keyTag, field, jsonStr)
	return err
}

//set redis config
func (f *FileData) SetRedisConf(cfg *conf.RedisConfig) {
	//check and setup
	if f.initDone {
		return
	}
	f.cfg = cfg

	//gen redis conf
	redisConf := f.GenRedisConf(f.cfg)

	//init base data
	f.hash = base.NewHashData(redisConf)
	f.sorted = base.NewSortedData(redisConf)

	//inter data init done
	f.initDone = true
}

//////////////////
//private func
//////////////////

//get removed file base key tag
func (f *FileData) getRemovedFileBaseKey() string {
	return define.RedisKeyRemovedFileBase
}

//get file short url list key tag
func (f *FileData) getFileListKey() string {
	return define.RedisKeyFilesList
}

//get file info key tag
func (f *FileData) getFileInfoKey(shortUrl string) (string, error) {
	//check
	if shortUrl == "" {
		return "", errors.New("invalid parameter")
	}
	//get hash key index
	hashIdx, err := f.getHashIdx(shortUrl, define.RedisFileInfoKeyNum)
	if err != nil {
		return "", err
	}
	//gen final key tag name
	keyTag := fmt.Sprintf(define.RedisKeyFileInfoPattern, hashIdx)
	return keyTag, nil
}

//get file base key tag
func (f *FileData) getFileBaseKey(md5 string) (string, error) {
	//check
	if md5 == "" {
		return "", errors.New("invalid parameter")
	}
	//get hash key index
	hashIdx, err := f.getHashIdx(md5, define.RedisFileBaseKeyNum)
	if err != nil {
		return "", err
	}
	//gen final key tag name
	keyTag := fmt.Sprintf(define.RedisKeyFileBasePattern, hashIdx)
	return keyTag, nil
}

//get hash key index
//use first two char ascii value as hash value
func (f *FileData) getHashIdx(input string, keyNum int) (int, error) {
	//check
	if input == "" || keyNum <= 0 {
		return 0, errors.New("invalid parameter")
	}
	//calculate hash value and idx
	hashBaseVal, _ := f.GetAsciiValue(input, define.AsciiCharSize)
	hashIdx := hashBaseVal % keyNum
	return hashIdx, nil
}