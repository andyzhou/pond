package testing

import (
	"fmt"
	"github.com/andyzhou/pond"
	"os"
)

const (
	DataDir = "../private"
	ShortUrl = "dkU9Ud" //"dMVRt8"
)

//init pond
func InitPond() (*pond.Pond, error) {
	//init face
	p := pond.NewPond()

	//get current path
	curPath, err := os.Getwd()
	if err != nil {
		return nil, err
	}
	dataPath := fmt.Sprintf("%v/%v", curPath, DataDir)

	//set config
	cfg := p.GenConfig()
	cfg.DataPath = dataPath
	cfg.FixedBlockSize = true
	cfg.CheckSame = true
	cfg.InterQueueSize = 2048

	//set config
	err = p.SetConfig(cfg)
	if err != nil {
		return nil, err
	}

	return p, err
}
