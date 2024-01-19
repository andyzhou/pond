package chunk

import (
	"errors"
	"time"
)

/*
 * chunk read face
 * - support queue and direct opt
 */

//read file
func (f *Chunk) ReadFile(
			offset,
			size int64,
		) ([]byte, error) {
	//check
	if offset < 0 || size <= 0 {
		return nil, errors.New("invalid parameter")
	}

	//check lazy mode
	if !f.readLazy {
		//direct read data
		return f.directReadData(offset, size)
	}

	//init read request
	req := ReadReq{
		Offset: offset,
		Size: size,
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
	size := req.Size

	//direct read data
	byteData, err := f.directReadData(offset, size)
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
func (f *Chunk) directReadData(
	offset, size int64) ([]byte, error) {
	//check
	if offset < 0 || size <= 0 {
		return nil, errors.New("invalid parameter")
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

	//create block buffer
	byteData := make([]byte, size)

	//read real data and sync active time
	f.fileLocker.Lock()
	defer f.fileLocker.Unlock()
	_, err := f.file.ReadAt(byteData, offset)
	f.lastActiveTime = time.Now().Unix()
	return byteData, err
}