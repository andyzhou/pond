package chunk

import (
	"fmt"
	"github.com/andyzhou/pond/define"
	"os"
	"time"
)

/*
 * chunk data file face
 */

//close chunk data file
func (f *Chunk) closeDataFile() error {
	//check
	if !f.openDone {
		return fmt.Errorf("chunk file %v not opened", f.file)
	}

	//close file obj
	if f.file != nil {
		//force update meta data
		f.updateMetaFile(true)
		f.file.Close()
		f.file = nil
	}

	//close chan notify
	if f.readCloseChan != nil {
		f.readCloseChan <- true
	}
	if f.writeCloseChan != nil {
		f.writeCloseChan <- true
	}

	//defer
	defer func() {
		f.openDone = false
	}()

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

	//sync file handle
	f.file = file
	f.openDone = true
	f.lastActiveTime = time.Now().Unix()

	if f.isLazyMode {
		//start write process
		go f.writeProcess()

		//start read process
		go f.readProcess()
	}
	return nil
}