package chunk

import (
	"errors"
	"github.com/andyzhou/pond/define"
	"github.com/andyzhou/pond/face"
	"github.com/andyzhou/pond/json"
	"log"
	"os"
	"time"
)

/*
 * chunk meta file opt face
 */

//meta auto save process
func (f *Chunk) saveMetaProcess() {
	var (
		ticker = time.NewTicker(define.ChunkFileMetaSaveRate * time.Second)
		m any = nil
	)

	//defer
	defer func() {
		if err := recover(); err != m {
			log.Printf("chunk.saveMetaProcess panic, err:%v\n", err)
		}

		//force save meta data
		f.updateMetaFile(true)

		//close ticker
		ticker.Stop()
	}()

	//loop
	for {
		select {
		case <- ticker.C:
			{
				if !f.metaUpdated {
					f.updateMetaFile()
				}
			}
		case <- f.metaCloseChan:
			return
		}
	}
}

//update meta file
func (f *Chunk) updateMetaFile(isForces ...bool) error {
	var (
		isForce bool
	)
	//check
	if f.metaFilePath == "" || f.chunkObj == nil {
		return errors.New("inter data not init yet")
	}

	//detect
	if isForces != nil && len(isForces) > 0 {
		isForce = isForces[0]
	}
	if !isForce {
		//just update switcher
		f.metaUpdated = false
		return nil
	}

	//force save meta data
	gob := face.GetFace().GetGob()
	err := gob.Store(f.metaFilePath, f.chunkObj)
	if err != nil {
		log.Printf("chunk.writeData, update meta failed, err:%v\n", err.Error())
	}
	f.metaUpdated = true
	return err
}

//open chunk data file
func (f *Chunk) openDataFile() error {
	//open real file, auto create if not exists
	file, err := os.OpenFile(f.dataFilePath, os.O_CREATE|os.O_RDWR, define.FilePerm)
	if err != nil {
		return err
	}

	//sync file handle
	f.file = file
	f.openDone = true

	if f.isLazyMode {
		//start write process
		go f.writeProcess()

		//start read process
		go f.readProcess()
	}
	return nil
}

//load chunk meta file
func (f *Chunk) loadMetaFile() error {
	//load god file
	gob := face.GetFace().GetGob()
	chunkObj := json.NewChunkFileJson()
	err := gob.Load(f.metaFilePath, &chunkObj)
	if err != nil {
		return err
	}

	//sync chunk obj
	f.chunkObj = chunkObj
	if f.chunkObj != nil {
		needUpdate := false
		if f.chunkObj.Id <= 0 {
			f.chunkObj.Id = f.chunkFileId
			needUpdate = true
		}
		if f.chunkObj.MaxSize <= 0 {
			f.chunkObj.MaxSize = define.DefaultChunkMaxSize
			needUpdate = true
		}
		if needUpdate {
			f.updateMetaFile()
		}
	}
	return nil
}