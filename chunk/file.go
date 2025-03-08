package chunk

import (
	"fmt"
	"log"
	"os"
	"runtime"
	"time"

	"github.com/andyzhou/pond/define"
	"golang.org/x/sys/unix"
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
		if f.cfg.UseMemoryMap {
			//sync to file data
			err := unix.Fsync(int(f.file.Fd()))
			if err != nil {
				log.Printf("chunk file %v sync failed, err:%v\n", f.chunkObj.Id, err.Error())
			}

			//release memory map data
			unix.Munmap(f.data)
		}

		//force update meta data
		f.updateMetaFile(true)
		f.file.Close()
		f.file = nil
	}
	f.openDone = false

	//gc opt
	runtime.GC()
	return nil
}

//open chunk data file
func (f *Chunk) openDataFile() error {
	//check
	if f.openDone && f.file != nil {
		return fmt.Errorf("chunk file %v had opened", f.file)
	}

	//open real file, auto create if not exists
	file, err := os.OpenFile(f.dataFilePath, os.O_RDWR|os.O_CREATE, define.FilePerm)
	if err != nil {
		return err
	}

	//get file size
	fileInfo, subErr := file.Stat()
	if subErr != nil {
		return subErr
	}
	fileSize := fileInfo.Size()

	//file data opt with locker
	f.fileLocker.Lock()
	defer f.fileLocker.Unlock()

	if f.cfg.UseMemoryMap {
		//use memory map data mode
		//init empty file
		if fileSize <= 0 {
			//write empty data header
			md5Str := fmt.Sprintf("%v", time.Now().Unix())
			headerData := f.genRealHeaderData(md5Str, []byte(md5Str), 0)
			_, err = file.WriteAt(headerData, 0)
			if err != nil {
				return err
			}
			fileSize = int64(len(headerData))
		}

		//init memory data map
		data, subErrOne := unix.Mmap(int(file.Fd()), 0, int(fileSize), unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED)
		if subErrOne != nil {
			return subErrOne
		}
		f.data = data

		//let system core use rule of `sequential`, do not cache huge data
		//this will reduce system memory occupy
		unix.Madvise(f.data, unix.MADV_SEQUENTIAL)

		//keep file active
		runtime.KeepAlive(f.file)
	}

	//sync file handle
	f.file = file
	f.openDone = true
	f.lastActiveTime = time.Now().Unix()
	return nil
}