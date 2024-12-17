package chunk

import (
	"errors"

	"github.com/andyzhou/pond/face"
)

/*
 * packet header data opt
 * @author <AndyZhou>
 * @mail <diudiu8848@163.com>
 */

//unpack header
func (f *Chunk) unpackHeader(data []byte) (face.IMessage, error) {
	//check
	if data == nil || len(data) <= 0 {
		return nil, errors.New("invalid parameter")
	}

	//un-pack header
	pack := face.NewPacket()
	msg, err := pack.UnPack(data)
	return msg, err
}

//pack header
func (f *Chunk) packHeader(md5 string, blocks, size int64) ([]byte, error) {
	//check
	if md5 == "" || size <= 0 {
		return nil, errors.New("invalid parameter")
	}

	//init new message
	msg := face.NewMessage()
	msg.SetMd5(md5)
	msg.SetBlocks(blocks)
	msg.SetLen(size)

	//pack header
	pack := face.NewPacket()
	data, err := pack.Pack(msg)
	return data, err
}