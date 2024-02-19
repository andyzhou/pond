package main

import (
	"fmt"
	"github.com/andyzhou/pond"
	"github.com/andyzhou/pond/define"
	"log"
	"os"
)

/*
 * example code
 * @author <AndyZhou>
 * @mail <diudiu8848@163.com>
 */

const (
	RedisAddr = "127.0.0.1:6379"
	DataDir = "./private"
	ShortUrl = "4Al0Yt"
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
	data := []byte(fmt.Sprintf("hello-%v", 2))
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
	//init face
	p := pond.NewPond()

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
	cfg.WriteLazy = true
	cfg.FixedBlockSize = true
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

	//file info list
	//getFileInfos(p)

	//write data
	shortUrl, _ := writeData(p)
	log.Printf("write data, short url:%v\n", shortUrl)

	//read data
	readData(p, shortUrl)

	//del data
	//delData(p, ShortUrl)

	//quit
	p.Quit()

	p.Wait()
	log.Printf("example done")
}
