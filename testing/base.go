package testing

import (
	"fmt"
	"log"
	"os"
	"sync"

	"github.com/andyzhou/pond"
)

const (
	RedisAddr = "127.0.0.1:6379"
	DataDir = "../private"
	ShortUrl = "4swdwl"
)

var (
	p *pond.Pond
	_once sync.Once
)

//get single instance
func GetPond() *pond.Pond {
	var (
		err error
	)
	_once.Do(func() {
		p, err = initPond()
		if err != nil {
			panic(any(err))
		}
	})
	return p
}

//init pond
func initPond() (*pond.Pond, error) {
	//init face
	log.Printf("init pond...\n")
	pObj := pond.NewPond()

	//get current path
	curPath, err := os.Getwd()
	if err != nil {
		return nil, err
	}
	dataPath := fmt.Sprintf("%v/%v", curPath, DataDir)

	//set config
	cfg := pObj.GenConfig()
	cfg.DataPath = dataPath
	cfg.FixedBlockSize = true
	cfg.CheckSame = true
	cfg.UseMemoryMap = true

	//set redis config
	redisCfg := p.GenRedisConfig()
	redisCfg.GroupTag = "gen"
	redisCfg.Address = RedisAddr
	redisCfg.Pools = 5

	//set config
	err = pObj.SetConfig(cfg, redisCfg)
	if err != nil {
		return nil, err
	}

	return pObj, err
}
