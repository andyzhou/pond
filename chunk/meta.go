package chunk

import (
	"errors"
	"log"

	"github.com/andyzhou/pond/define"
	"github.com/andyzhou/pond/json"
)

/*
 * chunk meta file opt face
 * @author <AndyZhou>
 * @mail <diudiu8848@163.com>
 * - one chunk file, one meta file
 */

//cb for update meta file
func (f *Chunk) cbForUpdateMeta(inputs ...interface{}) error {
	if f.metaUpdated {
		return errors.New("meta file had updated")
	}
	err := f.updateMetaFile(true)
	return err
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
		f.metaLocker.Lock()
		defer f.metaLocker.Unlock()
		f.metaUpdated = false
		return nil
	}

	//force save meta data with locker
	f.metaLocker.Lock()
	defer f.metaLocker.Unlock()
	err := f.gob.Store(f.metaFilePath, f.chunkObj)
	if err != nil {
		log.Printf("chunk.writeData, update meta failed, err:%v\n", err.Error())
	}
	f.metaUpdated = true
	return err
}

//load chunk meta file
func (f *Chunk) loadMetaFile() error {
	//load god file with locker
	f.metaLocker.Lock()
	chunkObj := json.NewChunkFileJson()
	err := f.gob.Load(f.metaFilePath, &chunkObj)
	f.metaLocker.Unlock()
	if err != nil {
		//init default value
		f.chunkObj = json.NewChunkFileJson()
	}else{
		f.chunkObj = chunkObj
	}

	//sync chunk obj
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