package search

import (
	"errors"
	"github.com/andyzhou/pond/define"
	"github.com/andyzhou/pond/json"
	"github.com/andyzhou/tinylib/queue"
	"github.com/andyzhou/tinysearch"
	tDefine "github.com/andyzhou/tinysearch/define"
	tJson "github.com/andyzhou/tinysearch/json"
)

/*
 * file base info search face
 * @author <AndyZhou>
 * @mail <diudiu8848@163.com>
 * - file md5 value as primary key
 */

//face info
type FileBase struct {
	ts        *tinysearch.Service //reference
	queue     *queue.Queue
	queueSize int
}

//construct
func NewFileBase(
	ts *tinysearch.Service,
	queueSizes ...int) *FileBase {
	var (
		queueSize int
	)
	if queueSizes != nil && len(queueSizes) > 0 {
		queueSize = queueSizes[0]
	}

	//self init
	this := &FileBase{
		ts: ts,
		queueSize: queueSize,
	}
	this.interInit()
	return this
}

//quit
func (f *FileBase) Quit() {
	if f.queue != nil {
		f.queue.Quit()
	}
}

//get batch filter by removed and sort by blocks
//sync opt
func (f *FileBase) GetBatchByBlocks(
		blocksMin, blocksMax int64,
		pageSize int,
	) (int64, []*json.FileBaseJson, error) {
	//check
	if blocksMin <= 0 || blocksMax <= blocksMin {
		return 0, nil, errors.New("invalid parameter")
	}
	if pageSize <= 0 {
		pageSize = define.DefaultPageSize
	}
	page := define.DefaultPage

	//setup filters
	filters := make([]*tJson.FilterField, 0)
	filterByRemoved := &tJson.FilterField{
		Kind: tDefine.FilterKindBoolean,
		Field: define.SearchFieldOfRemoved,
		IsMust: true,
	}
	filterByBlocks := &tJson.FilterField{
		Kind: tDefine.FilterKindNumericRange,
		Field: define.SearchFieldOfBlocks,
		MinFloatVal: float64(blocksMin),
		MaxFloatVal: float64(blocksMax),
		IsMust: true,
	}
	filters = append(filters, filterByRemoved, filterByBlocks)

	//setup sorts
	//sort by blocks asc
	sorts := make([]*tJson.SortField, 0)
	sortByBlocks := &tJson.SortField{
		Field: define.SearchFieldOfBlocks,
	}
	sorts = append(sorts, sortByBlocks)

	//call base func
	return f.QueryBatch(filters, sorts, page, pageSize)
}

//get batch removed blocks
//sync opt
func (f *FileBase) GetBatchByRemoved(
		page, pageSize int,
	) (int64, []*json.FileBaseJson, error) {
	//check
	if page <= 0 {
		page = define.DefaultPage
	}
	if pageSize <= 0 {
		pageSize = define.DefaultPageSize
	}

	//setup filters
	filters := make([]*tJson.FilterField, 0)

	//filter by removed
	filterByRemoved := &tJson.FilterField{
		Kind: tDefine.FilterKindBoolean,
		Field: define.SearchFieldOfRemoved,
		Val: true,
		IsMust: true,
	}
	filters = append(filters, filterByRemoved)

	//setup sorts
	//sort by blocks asc
	sorts := make([]*tJson.SortField, 0)
	sortByBlocks := &tJson.SortField{
		Field: define.SearchFieldOfBlocks,
	}
	sorts = append(sorts, sortByBlocks)

	//call base func
	return f.QueryBatch(filters, sorts, page, pageSize)
}

//get batch info
//sort by block size asc
//sync opt
func (f *FileBase) QueryBatch(
		filters []*tJson.FilterField,
		sorts []*tJson.SortField,
		page,
		pageSize int,
	) (int64, []*json.FileBaseJson, error) {
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
	index := f.ts.GetIndex(define.SearchIndexOfFileBase)

	//search data
	query := f.ts.GetQuery()
	resultSlice, err := query.Query(index, queryOpt)
	if err != nil || resultSlice == nil || resultSlice.Total <= 0 {
		return 0, nil, err
	}

	//format result
	result := make([]*json.FileBaseJson, 0)
	total := int64(resultSlice.Total)
	for _, v := range resultSlice.Records {
		if v == nil || v.OrgJson == nil {
			total--
			continue
		}
		baseObj := json.NewFileBaseJson()
		baseObj.Decode(v.OrgJson, baseObj)
		if baseObj == nil || baseObj.Md5 == "" {
			total--
			continue
		}
		result = append(result, baseObj)
	}
	return total, result, nil
}

//get one base file info
//sync opt
func (f *FileBase) GetOne(md5Val string) (*json.FileBaseJson, error) {
	//check
	if md5Val == "" {
		return nil, errors.New("invalid parameter")
	}
	if f.ts == nil {
		return nil, errors.New("inter search engine not init")
	}

	//get relate face
	index := f.ts.GetIndex(define.SearchIndexOfFileBase)
	doc := f.ts.GetDoc()

	//get data by id
	hitDoc, err := doc.GetDoc(index, md5Val)
	if err != nil {
		return nil, err
	}
	if hitDoc == nil {
		return nil, nil
	}

	//decode json
	fileBaseJson := json.NewFileBaseJson()
	err = fileBaseJson.Decode(hitDoc.OrgJson, fileBaseJson)
	return fileBaseJson, err
}

//del one base file info
//async opt
func (f *FileBase) DelOne(md5 string) error {
	var (
		err error
	)
	//check
	if md5 == "" {
		return errors.New("invalid parameter")
	}
	if f.ts == nil {
		return errors.New("inter search engine not init")
	}
	if f.queueSize > 0 {
		if f.queue == nil {
			return errors.New("inter queue is nil or closed")
		}
		//run in queue
		_, err = f.queue.SendData(md5)
	}else{
		//direct call
		err = f.delOneBase(md5)
	}
	return err
}

//add one base file info
//async opt
func (f *FileBase) AddOne(obj *json.FileBaseJson) error {
	var (
		err error
	)
	//check
	if obj == nil || obj.Md5 == "" {
		return errors.New("invalid parameter")
	}
	if f.ts == nil {
		return errors.New("inter search engine not init")
	}
	if f.queueSize > 0 {
		if f.queue == nil {
			return errors.New("inter queue is nil or closed")
		}
		//run in queue
		_, err = f.queue.SendData(obj)
	}else{
		//direct run
		err = f.addOneBase(obj)
	}
	return err
}

////////////////
//private func
////////////////

//del one base info
func (f *FileBase) delOneBase(md5 string) error {
	//check
	if md5 == "" {
		return errors.New("invalid parameter")
	}
	if f.ts == nil {
		return errors.New("inter search engine not init")
	}

	//get relate face
	index := f.ts.GetIndex(define.SearchIndexOfFileBase)
	doc := f.ts.GetDoc()

	//del doc
	err := doc.RemoveDoc(index, md5)
	return err
}

//add one base info
func (f *FileBase) addOneBase(obj *json.FileBaseJson) error {
	//check
	if obj == nil || obj.Md5 == "" {
		return errors.New("invalid parameter")
	}
	if f.ts == nil {
		return errors.New("inter search engine not init")
	}

	//get relate face
	index := f.ts.GetIndex(define.SearchIndexOfFileBase)
	doc := f.ts.GetDoc()

	//add doc
	err := doc.AddDoc(index, obj.Md5, obj)
	return err
}

//cb for queue opt
func (f *FileBase) cbForQueueOpt(
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
	case *json.FileBaseJson:
		{
			//for save opt
			obj, _ := data.(*json.FileBaseJson)
			err = f.addOneBase(obj)
		}
	case string:
		{
			//for delete opt
			md5, _ := data.(string)
			err = f.delOneBase(md5)
		}
	default:
		{
			err = errors.New("invalid data type")
		}
	}
	return nil, err
}

//init index
func (f *FileBase) initIndex() {
	if f.ts == nil {
		return
	}
	//add index
	err := f.ts.AddIndex(define.SearchIndexOfFileBase)
	if err != nil {
		panic(any(err))
	}
}

//inter init
func (f *FileBase) interInit() {
	//init index
	f.initIndex()

	//check and init queue
	if f.queueSize > 0 {
		//init new queue
		f.queue = queue.NewQueue(f.queueSize)

		//set cb for queue opt
		f.queue.SetCallback(f.cbForQueueOpt)
	}
}
