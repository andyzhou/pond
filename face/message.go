package face

/*
 * message data face
 * @author <AndyZhou>
 * @mail <diudiu8848@163.com>
 * - application `IMessage`
 */

//face info
type Message struct {
	md5		string
	id	 	int64
	blocks  int64
	length  int64
}

//construct
func NewMessage() *Message {
	this := &Message{}
	return this
}

//get opt
func (f *Message) GetLen() int64 {
	return f.length
}
func (f *Message) GetBlocks() int64 {
	return f.blocks
}
func (f *Message) GetId() int64 {
	return f.id
}
func (f *Message) GetMd5() string {
	return f.md5
}

//set opt
func (f *Message) SetLen(val int64) {
	f.length = val
}
func (f *Message) SetBlocks(val int64) {
	f.blocks = val
}
func (f *Message) SetId(val int64) {
	f.id = val
}
func (f *Message) SetMd5(val string) {
	f.md5 = val
}