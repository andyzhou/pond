package face

import (
	"bytes"
	"encoding/binary"
	"errors"

	"github.com/andyzhou/pond/define"
)

/*
 * packet data face
 * @author <AndyZhou>
 * @mail <diudiu8848@163.com>
 * - application `IPacket`
 */

//inter macro define
//DO NOT CHANGE THIS!!!
//md5(32byte) + dataId(8byte) + blocks(8byte) + dataLen(8byte)
const (
	Md5Size = 32
	PacketHeadSize = 24 + Md5Size
)

//face info
type Packet struct {
	maxPackSize  int64
	littleEndian bool
	byteOrder    binary.ByteOrder
}

//construct
func NewPacket() *Packet {
	this := &Packet{
		maxPackSize: define.DefaultPacketMaxSize,
		littleEndian: true,
		byteOrder: binary.LittleEndian,
	}
	return this
}

//pack & unpack opt
//un-pack header bytes
func (f *Packet) UnPack(header []byte) (IMessage, error) {
	var (
		md5 = make([]byte, Md5Size)
		dataId int64
		blocks int64
		length int64
	)
	//check
	if header == nil || len(header) < PacketHeadSize {
		return nil, errors.New("invalid parameter")
	}

	//init data buff
	dataBuff := bytes.NewReader(header)

	//read header
	//md5
	err := binary.Read(dataBuff, f.byteOrder, md5)
	if err != nil {
		return nil, err
	}

	//data id
	err = binary.Read(dataBuff, f.byteOrder, &dataId)
	if err != nil {
		return nil, err
	}

	//blocks
	err = binary.Read(dataBuff, f.byteOrder, &blocks)
	if err != nil {
		return nil, err
	}

	//length
	err = binary.Read(dataBuff, f.byteOrder, &length)
	if err != nil {
		return nil, err
	}

	////check length
	//if length > f.maxPackSizemaxPackSize {
	//	tips := fmt.Sprintf("too large message data received, message length:%d", length)
	//	return nil, errors.New(tips)
	//}

	//init message data
	message := NewMessage()
	message.SetMd5(string(md5))
	message.SetId(dataId)
	message.SetBlocks(blocks)
	message.SetLen(length)

	return message, nil
}

//pack header message
func (f *Packet) Pack(message IMessage) ([]byte, error) {
	//check
	if message == nil {
		return nil, errors.New("invalid parameter")
	}

	//init data buff
	dataBuff := bytes.NewBuffer(nil)

	//write header
	//md5
	md5Bytes := []byte(message.GetMd5())
	err := binary.Write(dataBuff, f.byteOrder, md5Bytes)
	if err != nil {
		return nil, err
	}

	//data id
	err = binary.Write(dataBuff, f.byteOrder, message.GetId())
	if err != nil {
		return nil, err
	}

	//blocks
	err = binary.Write(dataBuff, f.byteOrder, message.GetBlocks())
	if err != nil {
		return nil, err
	}

	//length
	err = binary.Write(dataBuff, f.byteOrder, message.GetLen())
	if err != nil {
		return nil, err
	}

	////write real data
	//err = binary.Write(dataBuff, f.byteOrder, message.GetData())
	//if err != nil {
	//	return nil, err
	//}
	return dataBuff.Bytes(), nil
}

//get opt
//get max pack size
func (f *Packet) GetMaxPackSize() int64 {
	return f.maxPackSize
}

//get header length
func (f *Packet) GetHeadLen() int64 {
	return PacketHeadSize
}

//set opt
//set max pack size
func (f *Packet) SetMaxPackSize(val int64) {
	f.maxPackSize = val
}

//set little endian
func (f *Packet) SetLittleEndian(littleEndian bool) {
	f.littleEndian = littleEndian
	if littleEndian {
		f.byteOrder = binary.LittleEndian
	}else{
		f.byteOrder = binary.BigEndian
	}
}