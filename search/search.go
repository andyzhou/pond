package search

import (
	"errors"
	"fmt"
	"github.com/andyzhou/pond/define"
	"github.com/andyzhou/pond/utils"
	"github.com/andyzhou/tinysearch"
	"sync"
)

/*
 * inter search face
 * @author <AndyZhou>
 * @mail <diudiu8848@163.com>
 * - all file info storage into local search
 * - only service current node data
 * - base on tiny search
 */

//global variable
var (
	_search     *Search
	_searchOnce sync.Once
)

//face info
type Search struct {
	rootPath  string
	queueSize int
	initDone  bool
	info      *FileInfo
	base      *FileBase
	ts        *tinysearch.Service
	utils.Utils
}

//get single instance
func GetSearch() *Search {
	_searchOnce.Do(func() {
		_search = NewSearch()
	})
	return _search
}

//construct
func NewSearch() *Search {
	this := &Search{}
	return this
}

//quit
func (f *Search) Quit() {
	if f.info != nil {
		f.info.Quit()
	}
	if f.base != nil {
		f.base.Quit()
	}
	if f.ts != nil {
		f.ts.Quit()
	}
}

//get relate face
func (f *Search) GetFileInfo() *FileInfo {
	return f.info
}

func (f *Search) GetFileBase() *FileBase {
	return f.base
}

//set root path
func (f *Search) SetCore(
	path string,
	queueSizes ...int) error {
	//check
	if path == "" {
		return errors.New("invalid path parameter")
	}
	if f.initDone {
		return nil
	}

	//setup queue size
	//if not set size, queue will be closed
	if queueSizes != nil && len(queueSizes) > 0 {
		f.queueSize = queueSizes[0]
	}

	//format search root path
	f.rootPath = fmt.Sprintf("%v/%v", path, define.SubDirOfSearch)

	//check and create sub dir
	err := f.CheckDir(f.rootPath)
	if err != nil {
		return err
	}

	//init inter search index
	f.initIndex()
	return nil
}

//init index
func (f *Search) initIndex() {
	defer func() {
		f.initDone = true
	}()

	//create search service
	f.ts = tinysearch.NewService()
	f.ts.SetDataPath(f.rootPath)

	//init file base and info
	f.base = NewFileBase(f.ts, f.queueSize)
	f.info = NewFileInfo(f.ts, f.queueSize)
}