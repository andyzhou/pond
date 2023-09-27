package testing

import (
	"fmt"
	"github.com/andyzhou/pond"
	"os"
)

const (
	DataDir = "../private"
	ShortUrl = "EVBJFp" //"dMVRt8"
)

//init pond
func InitPond() (*pond.Pond, error) {
	//init face
	p := pond.GetPond()

	//get current path
	curPath, err := os.Getwd()
	if err != nil {
		return nil, err
	}
	dataPath := fmt.Sprintf("%v/%v", curPath, DataDir)

	//set root path
	err = p.SetRootPath(dataPath)
	return p, err
}
