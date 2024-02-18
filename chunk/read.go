package chunk

import (
	"errors"
	"github.com/andyzhou/pond/face"
	"time"
)

/*
 * chunk read face
 * @author <AndyZhou>
 * @mail <diudiu8848@163.com>
 * - support queue and direct opt
 */

//read file
//if not read whole data, need skip header
func (f *Chunk) ReadFile(
			offset,
			end int64,
			skipHeaders ...bool,
		) ([]byte, error) {
	var (
		skipHeader bool
	)
	//check
	if offset < 0 || end <= 0 {
		return nil, errors.New("invalid parameter")
	}
	if skipHeaders != nil && len(skipHeaders) > 0 {
		skipHeader = skipHeaders[0]
	}

	//check lazy mode
	if !f.readLazy {
		//direct read data
		return f.directReadData(offset, end, skipHeaders...)
	}

	//init read request
	req := ReadReq{
		Offset: offset,
		End: end,
		SkipHeader: skipHeader,
	}

	//send request
	result, err := f.readQueue.SendData(req, true)
	if err != nil {
		return nil, err
	}
	respObj, ok := result.(ReadResp)
	if !ok || &respObj == nil {
		return nil, errors.New("invalid response data")
	}
	if respObj.Err != nil {
		return nil, respObj.Err
	}
	return respObj.Data, nil
}

/////////////////
//private func
/////////////////

//cb for read queue
func (f *Chunk) cbForReadOpt(
		data interface{},
	) (interface{}, error) {
	//check
	if data == nil {
		return nil, errors.New("invalid parameter")
	}
	req, ok := data.(ReadReq)
	if !ok || &req == nil {
		return nil, errors.New("data format should be `WriteReq`")
	}

	//get key data
	offset := req.Offset
	end := req.End
	skipHeader := req.SkipHeader

	//direct read data
	byteData, err := f.directReadData(offset, end, skipHeader)
	if err != nil {
		return nil, err
	}

	//format response
	resp := ReadResp{
		Data: byteData,
	}
	return resp, nil
}

//direct read file data
//if not read whole data, need skip header
func (f *Chunk) directReadData(
	offset, end int64,
	skipHeaders ...bool,
	) ([]byte, error) {
	var (
		skipHeader bool
	)
	//check
	if offset < 0 || end <= 0 {
		return nil, errors.New("invalid parameter")
	}
	if skipHeaders != nil && len(skipHeaders) > 0 {
		skipHeader = skipHeaders[0]
	}

	//check and open file
	if !f.IsOpened() {
		err := f.openDataFile()
		if err != nil {
			return nil, err
		}
	}

	if f.file == nil {
		return nil, errors.New("chunk file closed")
	}

	//get header message
	pack := face.NewPacket()
	headerLen := pack.GetHeadLen()

	//create block buffer
	size := end - offset
	byteData := make([]byte, size)

	//read real data and sync active time
	f.fileLocker.Lock()
	defer f.fileLocker.Unlock()

	if !skipHeader {
		//read and unpack header
		header := make([]byte, headerLen)
		_, err := f.file.ReadAt(header, offset)
		if err != nil {
			return nil, err
		}
		msg, subErr := pack.UnPack(header)
		if subErr != nil {
			return nil, err
		}
		if size > msg.GetLen() {
			return nil, errors.New("request size exceed data size")
		}
	}

	//read real data
	_, err := f.file.ReadAt(byteData, offset + headerLen)
	f.lastActiveTime = time.Now().Unix()
	return byteData, err
}