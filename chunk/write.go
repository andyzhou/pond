package chunk

import (
	"errors"
	"math"
	"time"
)

/*
 * chunk write face
 * - support queue and direct opt
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
	if !f.writeLazy {
		//direct write data
		return f.directWriteData(data, offsets...)
	}

	//detect offset
	if offsets != nil && len(offsets) > 0 {
		offset = offsets[0]
	}

	//init write request
	req := WriteReq{
		Offset: offset,
		Data: data,
	}

	//send request
	result, err := f.writeQueue.SendData(req, true)
	if err != nil {
		resp.Err = err
		return &resp
	}
	respObj, ok := result.(WriteResp)
	if !ok || &respObj == nil {
		resp.Err = errors.New("invalid response data")
		return &resp
	}
	return &respObj
}

/////////////////
//private func
/////////////////

//cb for write queue
func (f *Chunk) cbForWriteOpt(
		data interface{},
	) (interface{}, error) {
	//check
	if data == nil {
		return nil, errors.New("invalid parameter")
	}
	req, ok := data.(WriteReq)
	if !ok || &req == nil {
		return nil, errors.New("data format should be `WriteReq`")
	}

	//get key data
	offset := req.Offset
	realData := req.Data

	//direct write data
	resp := f.directWriteData(realData, offset)
	return *resp, nil
}

//direct write data
func (f *Chunk) directWriteData(
		data []byte,
		offsets ...int64,
	) *WriteResp {
	var (
		assignedOffset bool
		offset int64 = -1
		resp WriteResp
	)

	//check
	if data == nil {
		resp.Err = errors.New("invalid parameter")
		return &resp
	}

	//check and open file
	if !f.IsOpened() {
		err := f.openDataFile()
		if err != nil {
			resp.Err = err
			return &resp
		}
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
	}else{
		//assigned offset
		assignedOffset = true
	}

	//calculate real block size
	dataSize := float64(len(data))
	realBlocks := int64(math.Ceil(dataSize / float64(f.cfg.ChunkBlockSize)))

	//create block buffer
	realBlockSize := realBlocks * f.cfg.ChunkBlockSize
	byteData := make([]byte, realBlockSize)
	copy(byteData, data)

	//write block buffer data into chunk
	_, err := f.file.WriteAt(byteData, offset)
	chunkOldSize := f.chunkObj.Size
	f.lastActiveTime = time.Now().Unix()
	if err == nil {
		if !assignedOffset {
			//update chunk obj
			f.chunkObj.Files++
			f.chunkObj.Size += realBlockSize
			//update meta file
			f.updateMetaFile()
		}
	}

	//format resp
	resp.NewOffSet = chunkOldSize
	resp.BlockSize = realBlockSize
	if assignedOffset {
		resp.NewOffSet = offset
	}
	if err != nil {
		resp.Err = err
	}
	return &resp
}