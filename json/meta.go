package json

import "github.com/andyzhou/tinylib/util"

/*
 * meta json info
 * @author <AndyZhou>
 * @mail <diudiu8848@163.com>
 * - chunk and meta info
 * - all meta file storage as gob format.
 * - update data real time
 */

//chunk file meta json
//update value when write new file
type ChunkFileJson struct {
	Id      int64 `json:"id"`      //unique chunk file id
	Size    int64 `json:"size"`    //current size
	Files   int64 `json:"files"`   //total files
	MaxSize int64 `json:"maxSize"` //max allow size
	util.BaseJson
}

//all chunks meta snap json
type MetaJson struct {
	FileId  int64   `json:"fileId"`  //inter dynamic data file id
	ChunkId int64   `json:"chunkId"` //inter chunk storage file id
	Chunks  []int64 `json:"chunks"`  //active chunk file ids
	util.BaseJson
}

//construct
func NewChunkFileJson() *ChunkFileJson {
	this := &ChunkFileJson{}
	return this
}

func NewMetaJson() *MetaJson {
	this := &MetaJson{
		Chunks: []int64{},
	}
	return this
}