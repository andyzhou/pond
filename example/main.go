package main

import (
	"github.com/andyzhou/pond"
	"log"
)

const (
	DataPath = "/Volumes/Data/project/src/github.com/andyzhou/pond/private"
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

	//set root path
	err := p.SetRootPath(DataPath)
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
