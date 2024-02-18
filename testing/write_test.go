package testing

import (
	"fmt"
	"github.com/andyzhou/pond"
	"testing"
	"time"
)

/*
 * write testing code
 * @author <AndyZhou>
 * @mail <diudiu8848@163.com>
 */

//read data
func writeData(p *pond.Pond) (string, error) {
	//format data
	now := time.Now().Unix()
	data := fmt.Sprintf("hell-%v", now)
	return p.WriteData([]byte(data))
}

//test write api
func TestWrite(t *testing.T) {
	//init pond
	p, err := InitPond()
	if err != nil {
		t.Log(err.Error())
		return
	}

	//write data
	shortUrl, subErr := writeData(p)
	t.Logf("write data, shortUrl:%v, err:%v\n", shortUrl, subErr)
}

//bench write api
func BenchmarkWrite(b *testing.B) {
	var (
		err error
	)
	//set times
	b.N = 2000

	//reset timer
	b.ResetTimer()
	b.Logf("write bench mark start\n")

	//init pond
	p, err := InitPond()
	if err != nil {
		b.Log(err.Error())
		return
	}

	//wait a moment for pond init
	time.Sleep(time.Second)

	//defer p.Quit()
	failed := 0
	succeed := 0
	for n := 0; n < b.N; n++ {
		_, err = writeData(p)
		if err != nil {
			failed++
		}else{
			succeed++
		}
	}

	b.Logf("write bench mark finished, succeed:%v, failed:%v\n", succeed, failed)
	if err != nil {
		b.Error(err)
	}

	//quit
	p.Quit()
	b.Logf("write bench mark all done!\n")
}