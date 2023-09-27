package json

/*
 * meta json info
 * - chunk and meta info
 * - all meta file storage as gob format.
 * - update data real time
 */

//chunk file meta json
//update value when write new file
type ChunkFileJson struct {
	Id int64 `json:"id"` //unique chunk file id
	Size int64 `json:"size"` //current size
	Files int32 `json:"files"` //total files
	BaseJson
}

//meta snap json
//all chunk simple info
type MetaJson struct {
	FileId int64 `json:"fileId"` //inter dynamic data file id
	ChunkId int64 `json:"chunkId"` //inter chunk file id
	Chunks []int64 `json:"chunks"` //active chunk file ids
	BaseJson
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