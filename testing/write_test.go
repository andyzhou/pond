package testing

import (
	"errors"
	"fmt"
	"log"
	"testing"
	"time"

	"github.com/andyzhou/pond"
)

/*
 * write testing code
 * @author <AndyZhou>
 * @mail <diudiu8848@163.com>
 */

var (
	rounds = 0
	totalRounds = 3
)

//init
func init() {
	p = GetPond()
}

//read data
func writeData(p *pond.Pond) (string, error) {
	if p == nil {
		return "", errors.New("pond client not init")
	}
	//format data
	now := time.Now().Unix()
	data := fmt.Sprintf("hello-%v", now)
	return p.WriteData([]byte(data))
}

//test write api
func TestWrite(t *testing.T) {
	//write data
	shortUrl, subErr := writeData(p)
	t.Logf("write data, shortUrl:%v, err:%v\n", shortUrl, subErr)
}

//force close
func forceClose()  {
	//p.Quit()
	log.Printf("xxx")
}

//bench write api
func BenchmarkWrite(b *testing.B) {
	var (
		//shortUrl string
		err error
	)
	//set times
	//b.N = 1000

	//reset timer
	b.ResetTimer()

	failed := 0
	succeed := 0
	for n := 0; n < b.N; n++ {
		_, err = writeData(p)
		if err != nil {
			failed++
		}else{
			//b.Logf("n:%v, shortUrl:%v\n", n, shortUrl)
			succeed++
		}
	}

	b.Logf("write bench mark finished, succeed:%v, failed:%v\n", succeed, failed)
	if err != nil {
		b.Error(err)
	}

	//quit
	b.Logf("write bench mark all done!\n")
	rounds++

	if rounds >= totalRounds {
		p.Quit()
	}
}