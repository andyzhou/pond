package main

import (
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/andyzhou/pond"
	"github.com/andyzhou/pond/define"
)

/*
 * example code
 * @author <AndyZhou>
 * @mail <diudiu8848@163.com>
 */

const (
	RedisAddr = "127.0.0.1:6379"
	DataDir   = "./private"
)

//del data
func delData(p *pond.Pond, shortUrl string) error {
	err := p.DelData(shortUrl)
	log.Printf("del data, shortUrl:%v, err:%v\n", shortUrl, err)
	return err
}

//write data
func writeData(p *pond.Pond, shortUrls ...string) (string, error) {
	//now := time.Now().Unix()
	data := []byte(fmt.Sprintf("hello-%v", time.Now().Unix()))
	shortUrl, subErr := p.WriteData(data, shortUrls...)
	log.Printf("write data, shortUrl:%v, err:%v\n", shortUrl, subErr)
	return shortUrl, subErr
}

//read data
func readData(p *pond.Pond, shortUrl string) {
	if p == nil || shortUrl == "" {
		return
	}
	dataByte, err := p.ReadData(shortUrl)
	log.Printf("read data, shortUrl:%v, data:%v, err:%v\n", shortUrl, string(dataByte), err)
}

//get file info list
func getFileInfos(p *pond.Pond) {
	page := 1
	pageSize := 10
	total, recSlice, err := p.GetFiles(page, pageSize)
	log.Printf("files list, total:%v, recSlice:%v, err:%v\n", total, recSlice, err)
}

func main() {
	var (
		wg sync.WaitGroup
	)
	//init face
	p := pond.NewPond()
	time.Sleep(time.Second/10)

	//get current path
	curPath, err := os.Getwd()
	if err != nil {
		log.Println(err)
		return
	}
	dataPath := fmt.Sprintf("%v/%v", curPath, DataDir)

	//gen new config
	cfg := p.GenConfig()
	cfg.DataPath = dataPath
	cfg.CheckSame = true
	cfg.WriteLazy = false
	cfg.FixedBlockSize = true
	cfg.UseMemoryMap = true
	cfg.ChunkBlockSize = define.DefaultChunkBlockSize

	//gen new redis config
	redisCfg := p.GenRedisConfig()
	redisCfg.GroupTag = "gen"
	redisCfg.Address = RedisAddr
	redisCfg.Pools = 3

	//set config
	err = p.SetConfig(cfg, redisCfg)
	if err != nil {
		log.Println(err)
		return
	}

	//add group
	wg.Add(1)

	//file info list
	//getFileInfos(p)

	//write data
	shortUrl, _ := writeData(p)
	log.Printf("write data, short url:%v\n", shortUrl)

	//read data
	readData(p, shortUrl)

	//del data
	//delData(p, ShortUrl)

	//delay quit
	df := func() {
		wg.Done()
	}
	time.AfterFunc(time.Second * 3, df)

	wg.Wait()

	//quit
	p.Quit()
	log.Printf("example done")
}
