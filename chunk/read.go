package chunk

import (
	"errors"
	"github.com/andyzhou/pond/define"
	"log"
)

/*
 * chunk read face
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
	if !f.isLazyMode {
		//direct read data
		return f.directReadData(offset, size)
	}

	//for lazy mode
	if f.readChan == nil ||
		len(f.readChan) >= define.DefaultChunkChanSize {
		return nil, errors.New("chunk data read chan invalid")
	}

	//init read request
	req := ReadReq{
		Offset: offset,
		Size: size,
		Resp: make(chan ReadResp, 1),
	}

	//send request
	f.readChan <- req

	//wait for response
	resp, isOk := <- req.Resp
	if !isOk && &resp == nil {
		return nil, errors.New("can't get response data")
	}
	return resp.Data, resp.Err
}

/////////////////
//private func
/////////////////

//read process
func (f *Chunk) readProcess() {
	var (
		req  ReadReq
		isOk bool
		m    any = nil
	)

	//defer
	defer func() {
		if err := recover(); err != m {
			log.Printf("chunk.readProcess panic, err:%v\n", err)
		}
		//close chan
		close(f.readChan)
	}()

	//loop
	for {
		select {
		case req, isOk = <- f.readChan:
			if isOk {
				//read data
				data, err := f.readData(&req)

				//send response
				resp := ReadResp{
					Data: data,
					Err: err,
				}
				req.Resp <- resp
			}
		case <- f.readCloseChan:
			{
				return
			}
		}
	}
}

//read file data
func (f *Chunk) readData(req *ReadReq) ([]byte, error) {
	//check
	if req == nil || req.Offset < 0 || req.Size <= 0 {
		return nil, errors.New("invalid parameter")
	}
	return f.directReadData(req.Offset, req.Size)
}

//direct read file data
func (f *Chunk) directReadData(offset, size int64) ([]byte, error) {
	//check
	if offset < 0 || size <= 0 {
		return nil, errors.New("invalid parameter")
	}
	if f.file == nil {
		return nil, errors.New("chunk file closed")
	}

	//create block buffer
	byteData := make([]byte, size)

	//read real data
	_, err := f.file.ReadAt(byteData, offset)
	return byteData, err
}