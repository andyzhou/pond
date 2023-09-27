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

	//read data
	readData(p)

	//write data
	//writeData(p)

	//quit
	p.Quit()
}
