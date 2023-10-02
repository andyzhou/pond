package chunk

import (
	"errors"
	"github.com/andyzhou/pond/define"
	"log"
	"math"
)

/*
 * chunk write face
 */

//write file
//return ChunkWriteResp, error
func (f *Chunk) WriteFile(
			data []byte,
			offsets ...int64,
		) *WriteResp {
	var (
		offset int64 = -1
		resp   WriteResp
	)

	//check
	if data == nil {
		resp.Err = errors.New("invalid parameter")
		return &resp
	}

	//check lazy mode
	if !f.isLazyMode {
		//direct write data
		return f.directWriteData(data, offsets...)
	}

	if f.writeChan == nil || len(f.writeChan) >= define.DefaultChunkChanSize {
		resp.Err = errors.New("chunk data write chan invalid")
		return &resp
	}

	//detect offset
	if offsets != nil && len(offsets) > 0 {
		offset = offsets[0]
	}

	//init write request
	req := WriteReq{
		Offset: offset,
		Data: data,
		Resp: make(chan WriteResp, 1),
	}

	//send request
	f.writeChan <- req

	//wait for response
	resp, isOk := <- req.Resp
	if !isOk && &resp == nil {
		resp = WriteResp{}
		resp.Err = errors.New("can't get response data")
		return &resp
	}
	return &resp
}

/////////////////
//private func
/////////////////

//write process
func (f *Chunk) writeProcess() {
	var (
		req  WriteReq
		isOk bool
		m    any = nil
	)

	//defer
	defer func() {
		if err := recover(); err != m {
			log.Printf("chunk.writeProcess panic, err:%v\n", err)
		}

		//close chan
		close(f.writeChan)
	}()

	//loop
	for {
		select {
		case req, isOk = <- f.writeChan:
			if isOk {
				//write data
				resp := f.writeData(&req)
				//send response
				req.Resp <- *resp
			}
		case <- f.writeCloseChan:
			return
		}
	}
}

//write file data
func (f *Chunk) writeData(req *WriteReq) *WriteResp {
	var (
		resp WriteResp
	)
	//check
	if req == nil || req.Data == nil {
		resp.Err = errors.New("invalid parameter")
		return &resp
	}
	if f.file == nil {
		resp.Err = errors.New("chunk file closed")
		return &resp
	}

	//begin write data
	writeResp := f.directWriteData(req.Data, req.Offset)
	resp = *writeResp
	return &resp
}

//direct write data
func (f *Chunk) directWriteData(
			data []byte,
			offsets ...int64,
		) *WriteResp {
	var (
		offset int64 = -1
		resp WriteResp
	)

	//check
	if data == nil {
		resp.Err = errors.New("invalid parameter")
		return &resp
	}
	if f.file == nil {
		resp.Err = errors.New("chunk file closed")
		return &resp
	}

	//detect offset
	if offsets != nil && len(offsets) > 0 {
		offset = offsets[0]
	}
	if offset < 0 {
		offset = f.chunkObj.Size
	}

	//calculate real block size
	dataSize := float64(len(data))
	realBlocks := int64(math.Ceil(dataSize / float64(f.blockSize)))

	//create block buffer
	realBlockSize := realBlocks * f.blockSize
	byteData := make([]byte, realBlockSize)
	copy(byteData, data)

	//write block buffer data into chunk
	_, err := f.file.WriteAt(byteData, offset)
	chunkOldSize := f.chunkObj.Size
	if err == nil {
		//update chunk obj
		f.chunkObj.Files++
		f.chunkObj.Size += realBlockSize

		//update meta file
		f.updateMetaFile()
	}

	//format resp
	resp.Err = err
	resp.NewOffSet = chunkOldSize
	resp.BlockSize = realBlockSize
	return &resp
}