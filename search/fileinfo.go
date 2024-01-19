package search

import (
	"errors"
	"github.com/andyzhou/pond/define"
	"github.com/andyzhou/pond/json"
	"github.com/andyzhou/tinylib/queue"
	"github.com/andyzhou/tinysearch"
	tJson "github.com/andyzhou/tinysearch/json"
	"sync"
)

/*
 * file info search face
 * - file short url as primary key
 */

//face info
type FileInfo struct {
	ts *tinysearch.Service //reference
	wg *sync.WaitGroup //reference
	queue *queue.Queue //inter write or delete queue
}

//construct
func NewFileInfo(ts *tinysearch.Service, wg *sync.WaitGroup) *FileInfo {
	this := &FileInfo{
		ts: ts,
		wg: wg,
		queue: queue.NewQueue(),
	}
	this.interInit()
	return this
}

//quit
func (f *FileInfo) Quit() {
	f.queue.Quit()
}

//get batch by create at desc
func (f *FileInfo) GetBathByTime(
			page,
			pageSize int,
		) (int64, []*json.FileInfoJson, error) {
	//setup sorts
	sorts := make([]*tJson.SortField, 0)
	sortByTime := &tJson.SortField{
		Field: define.SearchFieldOfCreateAt,
		Desc: true,
	}
	sorts = append(sorts, sortByTime)

	//call base func
	return f.QueryBatch(nil, sorts, page, pageSize)
}

//get batch info
//sync opt
func (f *FileInfo) QueryBatch(
			filters []*tJson.FilterField,
			sorts []*tJson.SortField,
			page,
			pageSize int,
		) (int64, []*json.FileInfoJson, error) {
	//check
	if page <= 0 {
		page = define.DefaultPage
	}
	if pageSize <= 0 {
		pageSize = define.DefaultPageSize
	}

	//init query opt
	queryOpt := tJson.NewQueryOptJson()
	queryOpt.Filters = filters
	queryOpt.Sort = sorts
	queryOpt.Page = page
	queryOpt.PageSize = pageSize
	queryOpt.NeedDocs = true

	//get index
	index := f.ts.GetIndex(define.SearchIndexOfFileInfo)

	//search data
	query := f.ts.GetQuery()
	resultSlice, err := query.Query(index, queryOpt)
	if err != nil || resultSlice == nil || resultSlice.Total <= 0 {
		return 0, nil, err
	}

	//format result
	result := make([]*json.FileInfoJson, 0)
	total := int64(resultSlice.Total)
	for _, v := range resultSlice.Records {
		if v == nil || v.OrgJson == nil {
			total--
			continue
		}
		infoObj := json.NewFileInfoJson()
		infoObj.Decode(v.OrgJson, infoObj)
		if infoObj == nil || infoObj.ShortUrl == "" {
			total--
			continue
		}
		result = append(result, infoObj)
	}
	return total, result, nil
}

//get one file info
//sync opt
func (f *FileInfo) GetOne(shortUrl string) (*json.FileInfoJson, error) {
	//check
	if shortUrl == "" {
		return nil, errors.New("invalid parameter")
	}
	if f.ts == nil {
		return nil, errors.New("inter search engine not init")
	}

	//get relate face
	index := f.ts.GetIndex(define.SearchIndexOfFileInfo)
	doc := f.ts.GetDoc()

	//get data by short url
	hitDoc, err := doc.GetDoc(index, shortUrl)
	if err != nil {
		return nil, err
	}
	if hitDoc == nil {
		return nil, nil
	}

	//decode json
	fileInfoJson := json.NewFileInfoJson()
	err = fileInfoJson.Decode(hitDoc.OrgJson, fileInfoJson)
	return fileInfoJson, err
}

//del one file info
//async opt
func (f *FileInfo) DelOne(shortUrl string) error {
	//check
	if shortUrl == "" {
		return errors.New("invalid parameter")
	}
	if f.ts == nil {
		return errors.New("inter search engine not init")
	}

	//save into queue
	_, err := f.queue.SendData(shortUrl)
	return err
}

//add one file info
//async opt
func (f *FileInfo) AddOne(obj *json.FileInfoJson) error {
	//check
	if obj == nil || obj.ShortUrl == "" {
		return errors.New("invalid parameter")
	}
	if f.ts == nil {
		return errors.New("inter search engine not init")
	}

	//save into queue
	_, err := f.queue.SendData(obj)
	return err
}

////////////////
//private func
////////////////

//del one doc
func (f *FileInfo) delOneDoc(shortUrl string) error {
	//check
	if shortUrl == "" {
		return errors.New("invalid parameter")
	}
	if f.ts == nil {
		return errors.New("inter search engine not init")
	}

	//get relate face
	index := f.ts.GetIndex(define.SearchIndexOfFileInfo)
	doc := f.ts.GetDoc()

	//delete data by short url
	err := doc.RemoveDoc(index, shortUrl)
	return err
}

//add one doc
func (f *FileInfo) addOneDoc(obj *json.FileInfoJson) error {
	//check
	if obj == nil || obj.ShortUrl == "" {
		return errors.New("invalid parameter")
	}
	if f.ts == nil {
		return errors.New("inter search engine not init")
	}

	//get relate face
	index := f.ts.GetIndex(define.SearchIndexOfFileInfo)
	doc := f.ts.GetDoc()

	//add doc
	err := doc.AddDoc(index, obj.ShortUrl, obj)
	return err
}

//cb for queue quit
func (f *FileInfo) cbForQueueQuit() {
	if f.wg != nil {
		f.wg.Done()
	}
}

//cb for queue opt
func (f *FileInfo) cbForQueueOpt(
	data interface{}) (interface{}, error) {
	var (
		err error
	)
	//check
	if data == nil {
		return nil, errors.New("invalid parameter")
	}

	//do diff opt by data type
	switch data.(type) {
	case *json.FileInfoJson:
		{
			//for save opt
			obj, _ := data.(*json.FileInfoJson)
			err = f.addOneDoc(obj)
		}
	case string:
		{
			//for delete opt
			shortUrl, _ := data.(string)
			err = f.delOneDoc(shortUrl)
		}
	default:
		{
			err = errors.New("invalid data type")
		}
	}
	return nil, err
}

//init index
func (f *FileInfo) initIndex() {
	if f.ts == nil {
		return
	}
	//add index
	err := f.ts.AddIndex(define.SearchIndexOfFileInfo)
	if err != nil {
		panic(any(err))
	}
}

//inter init
func (f *FileInfo) interInit() {
	//init index
	f.initIndex()

	//set cb for queue
	f.queue.SetCallback(f.cbForQueueOpt)
	f.queue.SetQuitCallback(f.cbForQueueQuit)
	f.wg.Add(1)
}
