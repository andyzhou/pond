package face

import (
	"encoding/gob"
	"errors"
	"github.com/andyzhou/pond/define"
	"github.com/andyzhou/pond/util"
	"os"
	"path"
	"sync"
)

/*
 * gob data face
 * - opt gob data file
 * - suggest lazy store with rate
 */

//inter type
type (
	GobInfo struct {
		File *os.File
		Data interface{}
	}
)

//face info
type Gob struct {
	rootPath string
	filesMap sync.Map //filePath -> *GobInfo
	files int32
	util.Util
	sync.RWMutex
}

//construct
func NewGob() *Gob {
	this := &Gob{
		filesMap: sync.Map{},
	}
	this.interInit()
	return this
}

//quit
func (f *Gob) Quit() {
	if f.files <= 0 {
		return
	}
	//save and close files
	sf := func(k, v interface{}) bool {
		gobFile, _ := k.(string)
		gobInfo, _ := v.(*GobInfo)
		if gobFile != "" && gobInfo != nil {
			//save data, todo...
		}
		return true
	}
	f.filesMap.Range(sf)
}

//store chunk meta info
func (f *Gob) Store(gobFile string, inputVal interface{}) error {
	//check
	if gobFile == "" || inputVal == nil {
		return errors.New("invalid parameter")
	}

	//opt with locker
	f.Lock()
	defer f.Unlock()

	//format meta file path
	filePath := path.Join(f.rootPath, gobFile)
	file, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, define.FilePerm)
	if err != nil {
		return err
	}
	defer file.Close()

	//try encode gob file
	enc := gob.NewEncoder(file)
	err = enc.Encode(inputVal)
	return err
}

//load meta info
func (f *Gob) Load(gobFile string, outVal interface{}) error {
	//check
	if gobFile == "" || outVal == nil {
		return errors.New("invalid parameter")
	}

	//opt with locker
	f.Lock()
	defer f.Unlock()

	//format meta file path
	filePath := path.Join(f.rootPath, gobFile)

	//try open file
	file, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, define.FilePerm)
	if err != nil {
		return err
	}
	defer file.Close()

	//try decode gob file
	decoder := gob.NewDecoder(file)
	err = decoder.Decode(outVal)
	if err != nil && err.Error() == define.FileErrOfEOF {
		return nil
	}
	return err
}

//set root path
func (f *Gob) SetRootPath(path string) error {
	//check
	if path == "" {
		return errors.New("invalid path parameter")
	}
	f.rootPath = path
	return nil
}

////////////////
//private func
////////////////

//inter init
func (f *Gob) interInit() {
	//get work dir
	curPath, _ := f.GetCurDir()
	if curPath != "" {
		f.rootPath = curPath
	}
}