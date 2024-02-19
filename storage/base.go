package storage

import (
	"github.com/andyzhou/pond/data"
	"github.com/andyzhou/pond/json"
	"github.com/andyzhou/pond/search"
)

/*
 * storage base face
 */

type Base struct {
	useRedis bool
}

////////////////////////////
//api for file base and info
////////////////////////////

//del file base and info
func (f *Base) delFileInfo(shortUrl string) error {
	var (
		err error
	)
	if f.useRedis {
		//save into redis
		fileData := data.GetRedisData().GetFile()
		err = fileData.DelInfo(shortUrl)
	}else{
		//save into search
		fileInfoSearch := search.GetSearch().GetFileInfo()
		err = fileInfoSearch.DelOne(shortUrl)
	}
	return err
}
func (f *Base) delFileBase(md5 string) error {
	var (
		err error
	)
	if f.useRedis {
		//del from redis
		fileData := data.GetRedisData().GetFile()
		err = fileData.DelBase(md5)
	}else{
		//del from search
		fileBaseSearch := search.GetSearch().GetFileBase()
		err = fileBaseSearch.DelOne(md5)
	}
	return err
}

//get file base and info
func (f *Base) getFileInfo(shortUrl string) (*json.FileInfoJson, error) {
	var (
		fileInfoObj *json.FileInfoJson
		err error
	)
	if f.useRedis {
		//get from redis
		fileData := data.GetRedisData().GetFile()
		fileInfoObj, err = fileData.GetInfo(shortUrl)
	}else{
		//get from search
		fileInfoSearch := search.GetSearch().GetFileInfo()
		fileInfoObj, err = fileInfoSearch.GetOne(shortUrl)
	}
	return fileInfoObj, err
}
func (f *Base) getFileBase(md5 string) (*json.FileBaseJson, error) {
	var (
		fileBaseObj *json.FileBaseJson
		err error
	)
	if f.useRedis {
		//get from redis
		fileData := data.GetRedisData().GetFile()
		fileBaseObj, err = fileData.GetBase(md5)
	}else{
		//get from search
		fileBaseSearch := search.GetSearch().GetFileBase()
		fileBaseObj, err = fileBaseSearch.GetOne(md5)
	}
	return fileBaseObj, err
}

//save file base and info
func (f *Base) saveFileInfo(obj *json.FileInfoJson) error {
	var (
		err error
	)
	if f.useRedis {
		//save into redis
		fileData := data.GetRedisData().GetFile()
		err = fileData.AddInfo(obj)
	}else{
		//save into search
		fileInfoSearch := search.GetSearch().GetFileInfo()
		err = fileInfoSearch.AddOne(obj)
	}
	return err
}
func (f *Base) saveFileBase(obj *json.FileBaseJson) error {
	var (
		err error
	)
	if f.useRedis {
		//save into redis
		fileData := data.GetRedisData().GetFile()
		err = fileData.AddBase(obj)
	}else{
		//save into search
		fileBaseSearch := search.GetSearch().GetFileBase()
		err = fileBaseSearch.AddOne(obj)
	}
	return err
}