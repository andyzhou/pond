package json

import (
	"encoding/json"
	"errors"
)

/*
 * face for base json
 */

//face info
type BaseJson struct {
}

//construct
func NewBaseJson() *BaseJson {
	//self init
	this := &BaseJson{
	}
	return this
}

//encode json data
func (j *BaseJson) Encode(
			i interface{},
		) ([]byte, error) {
	if i == nil {
		return nil, errors.New("invalid parameter")
	}
	//encode json
	resp, err := json.Marshal(i)
	return resp, err
}

//decode json data
func (j *BaseJson) Decode(
			data []byte,
			i interface{},
		) error {
	if len(data) <= 0 {
		return errors.New("invalid parameter")
	}
	//try decode json data
	err := json.Unmarshal(data, i)
	return err
}

//encode simple kv data
func (j *BaseJson) EncodeSimple(
			data map[string]interface{},
		) ([]byte, error) {
	if data == nil {
		return nil, errors.New("invalid parameter")
	}
	//try encode json data
	dataBytes, err := json.Marshal(data)
	return dataBytes, err
}

//decode simple kv data
func (j *BaseJson) DecodeSimple(
			data []byte,
			kv map[string]interface{},
		) error {
	if len(data) <= 0 {
		return errors.New("invalid parameter")
	}
	//try decode json data
	err := json.Unmarshal(data, &kv)
	return err
}
