package pond

import (
	"github.com/andyzhou/pond/json"
	"github.com/andyzhou/pond/storage"
)

/*
 * main interface
 * - service one single node or group data
 */

//face info
type Pond struct {
	storage *storage.Storage
}

//construct
func NewPond() *Pond {
	this := &Pond{
		storage: storage.NewStorage(),
	}
	return this
}

//quit
func (f *Pond) Quit() {
	f.storage.Quit()
}

////////////////////
//api for file index
////////////////////

//get batch file info by create time
func (f *Pond) GetFiles(
			page, pageSize int,
		) (int64, []*json.FileInfoJson, error) {
	return f.storage.GetFilesInfo(page, pageSize)
}

////////////////////
//api for file data
////////////////////

//del data
func (f *Pond) DelRealData(shortUrl string) error {
	return f.storage.DelRealData(shortUrl)
}
func (f *Pond) DelData(shortUrl string) error {
	return f.storage.DeleteData(shortUrl)
}

//read data
//extend para: offset, length
func (f *Pond) ReadData(
			shortUrl string,
			offsetAndLength ...int64,
		) ([]byte, error) {
	return f.storage.ReadData(shortUrl, offsetAndLength...)
}

//write data
//return shortUrl, error
func (f *Pond) WriteData(data []byte) (string, error) {
	return f.storage.WriteData(data)
}

//set data root path
func (f *Pond) SetRootPath(path string) error {
	return f.storage.SetRootPath(path)
}