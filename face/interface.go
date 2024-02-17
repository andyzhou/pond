package face

/*
 * interface define
 * @author <AndyZhou>
 * @mail <diudiu8848@163.com>
 */

//message
type IMessage interface {
	//get
	GetMd5() string
	GetId() int64
	GetBlocks() int64
	GetLen() int64

	//set
	SetMd5(string)
	SetId(int64)
	SetBlocks(int64)
	SetLen(int64)
}

//packet
type IPacket interface {
	//pack & unpack
	UnPack(header []byte) (IMessage, error)
	Pack(message IMessage) ([]byte, error)

	//get opt
	GetHeadLen() int64
	GetMaxPackSize() int64

	//set opt
	SetMaxPackSize(size int64)
	SetLittleEndian(littleEndian bool)
}
