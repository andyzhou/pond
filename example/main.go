package main

import (
	"fmt"
	"github.com/andyzhou/pond"
	"github.com/andyzhou/pond/define"
	"log"
	"os"
	"time"
)

/*
 * example code
 * @author <AndyZhou>
 * @mail <diudiu8848@163.com>
 */

const (
	DataDir = "./private"
)

//write data
func writeData(p *pond.Pond, shortUrls ...string) (string, error) {
	data := []byte(fmt.Sprintf("hello-%v", time.Now().Unix()))
	shortUrl, subErr := p.WriteData(data, shortUrls...)
	log.Printf("shortUrl:%v, err:%v\n", shortUrl, subErr)
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
	cfg.FixedBlockSize = true
	cfg.ChunkBlockSize = define.DefaultChunkBlockSize

	//set config
	err = p.SetConfig(cfg)
	if err != nil {
		log.Println(err)
		return
	}

	//file info list
	//getFileInfos(p)

	//write data
	shortUrl, _ := writeData(p)

	//read data
	readData(p, shortUrl)

	//quit
	p.Quit()

	p.Wait()
	log.Printf("example done")
}
