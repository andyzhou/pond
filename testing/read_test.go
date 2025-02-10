package testing

import (
	"testing"

	"github.com/andyzhou/pond"
)

/*
 * read testing code
 * @author <AndyZhou>
 * @mail <diudiu8848@163.com>
 */

//init
func init()  {
	GetPond()
}

//read data
func readData(p *pond.Pond) ([]byte, error) {
	dataByte, err := p.ReadData(ShortUrl)
	return dataByte, err
}

//test read api
func TestRead(t *testing.T) {
	//read data
	//dataByte, err := readData(p)
	//t.Logf("read data, data:%v, err:%v\n", string(dataByte), err)
}

//bench read api
func BenchmarkRead(b *testing.B) {
	var (
		dataByte []byte
		err error
	)
	//set times
	//b.N = 1000

	//reset timer
	b.ResetTimer()

	failed := 0
	succeed := 0
	for n := 0; n < b.N; n++ {
		dataByte, err = readData(p)
		if err != nil {
			failed++
		}else{
			succeed++
		}
	}

	b.Logf("read bench mark finished, succeed:%v, failed:%v, dataByte:%v\n", succeed, failed, string(dataByte))
	if err != nil {
		b.Error(err)
	}

	//quit
	//p.Quit()
	//b.Logf("read bench mark all done!\n")
}