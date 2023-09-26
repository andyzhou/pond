package face

import (
	"encoding/gob"
	"errors"
	"github.com/andyzhou/pond/define"
	"os"
	"path"
	"sync"
)

/*
 * gob data face
 * - opt gob data file
 * - suggest lazy store with rate
 */

//face info
type Gob struct {
	rootPath string
	sync.RWMutex
}

//construct
func NewGob() *Gob {
	this := &Gob{
		rootPath: define.DefaultRootPath,
	}
	return this
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
	file, err := os.OpenFile(filePath, os.O_WRONLY|os.O_CREATE, define.FilePerm)
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
	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_RDWR, define.FilePerm)
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