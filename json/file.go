package json

/*
 * file json info
 * - all data storage in local search
 */

//file info json
type FileInfoJson struct {
	ShortUrl string `json:"shortUrl"` //unique url
	Name string `json:"name"`
	ContentType string `json:"contentType"`
	Size int64 `json:"size"`
	Md5 string `json:"md5"` //unique, base file md5
	ChunkFileId int64 `json:"chunkFileId"`
	Offset int64 `json:"offset"`
	Blocks int64 `json:"blocks"`
	CreateAt int64 `json:"createAt"`
	BaseJson
}

//file base json
//used for filter same content but diff name file
//md5 file content value as primary key
type FileBaseJson struct {
	Md5 string `json:"md5"` //primary key
	Size int64 `json:"size"`
	ChunkFileId int64 `json:"chunkFileId"` //chunk file id
	Offset int64 `json:"offset"`
	Blocks int64 `json:"blocks"`
	Removed bool `json:"removed"`
	Backed bool `json:"backed"` //backed or not
	CreateAt int64 `json:"createAt"`
	BaseJson
}

//construct
func NewFileInfoJson() *FileInfoJson {
	this := &FileInfoJson{}
	return this
}

func NewFileBaseJson() *FileBaseJson {
	this := &FileBaseJson{}
	return this
}