package chunk

import (
	"fmt"
	"github.com/andyzhou/pond/define"
	"os"
	"time"
)

/*
 * chunk data file base opt face
 * @author <AndyZhou>
 * @mail <diudiu8848@163.com>
 * - open and close chunk file
 */

//close chunk data file
func (f *Chunk) closeDataFile() error {
	//check
	if !f.openDone {
		return fmt.Errorf("chunk file %v not opened", f.file)
	}

	//do relate opt with locker
	f.fileLocker.Lock()
	defer f.fileLocker.Unlock()

	//close file obj
	if f.file != nil {
		//force update meta data
		f.updateMetaFile(true)
		f.file.Close()
		f.file = nil
	}
	f.openDone = false
	return nil
}

//open chunk data file
func (f *Chunk) openDataFile() error {
	//check
	if f.openDone {
		return fmt.Errorf("chunk file %v had opened", f.file)
	}

	//open real file, auto create if not exists
	file, err := os.OpenFile(f.dataFilePath, os.O_RDWR|os.O_CREATE, define.FilePerm)
	if err != nil {
		return err
	}

	//sync file handle with locker
	f.fileLocker.Lock()
	defer f.fileLocker.Unlock()
	f.file = file
	f.openDone = true
	f.lastActiveTime = time.Now().Unix()
	return nil
}