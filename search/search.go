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
 * - all file info storage into local search
 * - only service current node data
 * - base on tiny search
 */

//global variable
var (
	_search *Search
	_searchOnce sync.Once
)

//face info
type Search struct {
	rootPath string
	initDone bool
	info *FileInfo
	base *FileBase
	ts *tinysearch.Service
	wg *sync.WaitGroup //reference
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
	this := &Search{
	}
	return this
}

//quit
func (f *Search) Quit() {
	f.info.Quit()
	f.base.Quit()
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
func (f *Search) SetCore(path string, wg *sync.WaitGroup) error {
	//check
	if path == "" {
		return errors.New("invalid path parameter")
	}
	if f.initDone {
		return nil
	}
	f.wg = wg

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
	f.base = NewFileBase(f.ts, f.wg)
	f.info = NewFileInfo(f.ts, f.wg)
}