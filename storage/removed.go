package storage

import (
	"encoding/gob"
	"errors"
	"fmt"
	"github.com/andyzhou/pond/conf"
	"github.com/andyzhou/pond/define"
	"github.com/andyzhou/pond/json"
	"github.com/andyzhou/pond/utils"
	"github.com/andyzhou/tinylib/queue"
	"github.com/andyzhou/tinylib/util"
	"log"
	"sort"
	"sync"
)

/*
 * removed chunk base file info
 * - storage into local gob file
 * - cached in run env and auto sync into local
 */

//face info
type Removed struct {
	wg            *sync.WaitGroup     //reference
	cfg           *conf.Config        //reference
	removedSorter removedBaseFileSort //internal sorter
	removedJson   *json.RemovedJson
	gobFile       string
	gob           *util.Gob
	ticker        *queue.Ticker
	utils.Utils
	sync.RWMutex
}

//init
func init() {
	gob.Register(&json.RemovedJson{})
}

//construct
func NewRemoved(wg *sync.WaitGroup) *Removed {
	//self init
	this := &Removed{
		wg:          wg,
		removedJson: json.NewRemovedJson(),
		gob:         util.NewGob(),
	}
	this.interInit()
	return this
}

//close gob file
func (f *Removed) Quit() {
	defer func() {
		//force save data
		err := f.SaveRemoved()
		if err != nil {
			log.Printf("removed.Quit err:%v\n", err.Error())
		}
		if f.wg != nil {
			f.wg.Done()
			log.Println("storage.removed.cbForTickQuit")
		}
	}()
	if f.ticker != nil {
		f.ticker.Quit()
	}
}

//get available removed file base info
//return md5, error
func (f *Removed) GetAvailableRemovedFileBase(dataSize int64) (string, error) {
	var (
		matchedMd5 string
		matchedIdx = -1
		diff int
	)
	//check
	if dataSize <= 0 {
		return matchedMd5, errors.New("invalid parameter")
	}
	if f.removedSorter == nil || len(f.removedSorter) <= 0 {
		return matchedMd5, nil
	}

	//pick matched obj with locker
	f.Lock()
	defer f.Unlock()
	for idx, info := range f.removedSorter {
		if info.blocks >= dataSize {
			diff = int(info.blocks - dataSize)
			if diff <= define.DefaultChunkBlockSize {
				//matched
				matchedIdx = idx
				matchedMd5 = info.md5
				break
			}
		}
	}
	if matchedIdx < 0 {
		return matchedMd5, nil
	}

	//remove from obj
	delete(f.removedJson.BaseInfo, matchedMd5)

	//update sorter
	f.removedSorter = append(f.removedSorter[0:matchedIdx], f.removedSorter[matchedIdx+1:]...)

	return matchedMd5, nil
}

//add new removed base file
func (f *Removed) AddRemoved(md5 string, blocks int64) error {
	//check
	if md5 == "" || blocks <= 0 {
		return errors.New("invalid parameter")
	}

	//check exists with locker
	f.Lock()
	defer f.Unlock()
	_, ok := f.removedJson.BaseInfo[md5]
	if ok {
		return errors.New("need removed md5 exists")
	}

	//sync into run data
	f.removedJson.BaseInfo[md5] = blocks

	//add into sorter
	if f.removedSorter == nil {
		f.removedSorter = make([]*removedBaseFile, 0)
	}
	rf := &removedBaseFile{
		md5: md5,
		blocks: blocks,
	}
	f.removedSorter = append(f.removedSorter, rf)

	//sorter data
	sort.Sort(f.removedSorter)
	return nil
}

//set config
func (f *Removed) SetConfig(cfg *conf.Config) error {
	//check
	if cfg == nil {
		return errors.New("invalid parameter")
	}

	//sync config
	f.cfg = cfg

	//format file root path
	rootPath := fmt.Sprintf("%v/%v", cfg.DataPath, define.SubDirOfFile)

	//check and create sub dir
	err := f.CheckDir(rootPath)
	if err != nil {
		return err
	}

	//setup meta path and file
	f.gob.SetRootPath(rootPath)
	f.gobFile = define.ChunksRemovedFile

	//check and load meta file
	err = f.gob.Load(f.gobFile, &f.removedJson)
	if err != nil {
		f.removedJson = json.NewRemovedJson()
	}

	//fill inter sorter
	for k, v := range f.removedJson.BaseInfo {
		rf := &removedBaseFile{
			md5: k,
			blocks: v,
		}
		f.removedSorter = append(f.removedSorter, rf)
	}

	//re-sort
	sort.Sort(f.removedSorter)
	return nil
}

//save gob data
func (f *Removed) SaveRemoved() error {
	//check
	if f.gobFile == "" {
		return errors.New("removed gob file not setup")
	}

	//begin save data with locker
	f.Lock()
	defer f.Unlock()
	err := f.gob.Store(f.gobFile, f.removedJson)
	return err
}

//cb for auto save meta
func (f *Removed) cbForAutoSaveMeta(inputs ...interface{}) error {
	return f.SaveRemoved()
}

//inter init
func (f *Removed) interInit() {
	//init ticker
	f.ticker = queue.NewTicker(define.RemovedAutoSaveTicker)
	f.ticker.SetCheckerCallback(f.cbForAutoSaveMeta)

	//add wait group
	if f.wg != nil {
		f.wg.Add(1)
	}
}