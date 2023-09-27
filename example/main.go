package main

import (
	"fmt"
	"github.com/andyzhou/pond"
	"log"
	"os"
)

const (
	DataDir = "./private"
	ShortUrl = "EVBJFp" //"dMVRt8"
)

//write data
func writeData(p *pond.Pond) {
	data := []byte("3hello")
	shortUrl, subErr := p.WriteData(data)
	log.Printf("shortUrl:%v, err:%v\n", shortUrl, subErr)
}

//read data
func readData(p *pond.Pond) {
	dataByte, err := p.ReadData(ShortUrl)
	log.Printf("read data, data:%v, err:%v\n", string(dataByte), err)
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

	//set root path
	err = p.SetRootPath(dataPath, true)
	if err != nil {
		log.Println(err)
		return
	}

	//file info list
	getFileInfos(p)

	//read data
	//readData(p)

	//write data
	//writeData(p)

	//quit
	p.Quit()
}
