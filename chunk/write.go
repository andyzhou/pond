package chunk

import (
	"bytes"
	"errors"
	"math"
	"time"
)

/*
 * chunk write face
 * @author <AndyZhou>
 * @mail <diudiu8848@163.com>
 * - header + realData as whole data value
 * - support queue and direct opt
 */

//write file
//return ChunkWriteResp, error
func (f *Chunk) WriteFile(
		md5 string,
		data []byte,
		offsets ...int64,
	) *WriteResp {
	var (
		offset int64 = -1
		resp   WriteResp
	)

	//check
	if md5 == "" || data == nil {
		resp.Err = errors.New("invalid parameter")
		return &resp
	}

	//check lazy mode
	if !f.writeLazy {
		//direct write data
		return f.directWriteData(md5, data, offsets...)
	}

	//detect offset
	if offsets != nil && len(offsets) > 0 {
		offset = offsets[0]
	}

	//init write request
	req := WriteReq{
		Md5: md5,
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
	md5 := req.Md5
	offset := req.Offset
	realData := req.Data

	//direct write data
	resp := f.directWriteData(md5, realData, offset)
	return *resp, nil
}

//direct write data
func (f *Chunk) directWriteData(
		md5 string,
		data []byte,
		offsets ...int64,
	) *WriteResp {
	var (
		assignedOffset bool
		offset int64 = -1
		resp WriteResp
	)

	//check
	if md5 == "" || data == nil {
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
	dataLen := int64(len(data))
	dataSize := float64(dataLen)
	realBlocks := int64(math.Ceil(dataSize / float64(f.cfg.ChunkBlockSize)))

	//create block buffer
	realBlockSize := realBlocks * f.cfg.ChunkBlockSize

	//format header data
	header, _ := f.packHeader(md5, realBlockSize, dataLen)
	headerLen := len(header)

	//init whole data
	//format: header + realData
	byteDataLen := int64(headerLen) + realBlockSize
	byteData := make([]byte, byteDataLen)
	byteBuff := bytes.NewBuffer(nil)
	byteBuff.Write(header)
	byteBuff.Write(data)

	//copy whole data to dest byte buff
	copy(byteData, byteBuff.Bytes())

	//write block buffer data into chunk
	_, err := f.file.WriteAt(byteData, offset)
	if err != nil {
		resp.Err = err
		return &resp
	}

	oldOffset := f.chunkObj.Size
	f.lastActiveTime = time.Now().Unix()
	if !assignedOffset {
		//update chunk obj
		f.chunkObj.Files++
		f.chunkObj.Size += byteDataLen
		//update meta file
		err = f.updateMetaFile(true)
	}

	//format resp
	resp.NewOffSet = oldOffset
	resp.BlockSize = realBlockSize
	if assignedOffset {
		resp.NewOffSet = offset
	}
	return &resp
}