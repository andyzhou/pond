package search

import (
	"errors"
	"github.com/andyzhou/pond/define"
	"github.com/andyzhou/pond/json"
	"github.com/andyzhou/tinysearch"
	tDefine "github.com/andyzhou/tinysearch/define"
	tJson "github.com/andyzhou/tinysearch/json"
)

/*
 * file base info search face
 * - file md5 value as primary key
 */

//face info
type FileBase struct {
	ts *tinysearch.Service //reference
}

//construct
func NewFileBase(ts *tinysearch.Service) *FileBase {
	this := &FileBase{
		ts: ts,
	}
	this.interInit()
	return this
}

//get batch filter by removed and sort by blocks
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
func (f *FileBase) DelOne(md5 string) error {
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

//add one base file info
func (f *FileBase) AddOne(obj *json.FileBaseJson) error {
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

//inter init
func (f *FileBase) interInit() {
	if f.ts == nil {
		return
	}
	//add index
	err := f.ts.AddIndex(define.SearchIndexOfFileBase)
	if err != nil {
		panic(any(err))
	}
}
