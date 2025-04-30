package json

type RemovedJson struct {
	BaseInfo map[string]int64 `json:"baseInfo"` //md5 -> blocks
}

//construct
func NewRemovedJson() *RemovedJson {
	this := &RemovedJson{
		BaseInfo: map[string]int64{},
	}
	return this
}