package testing

import (
	"github.com/andyzhou/pond"
	"testing"
)

/*
 * read testing code
 * @author <AndyZhou>
 * @mail <diudiu8848@163.com>
 */

//read data
func readData(p *pond.Pond) ([]byte, error) {
	dataByte, err := p.ReadData(ShortUrl)
	return dataByte, err
}

//test read api
func TestRead(t *testing.T) {
	//init pond
	p, err := InitPond()
	if err != nil {
		t.Log(err.Error())
		return
	}

	//read data
	dataByte, err := readData(p)
	t.Logf("read data, data:%v, err:%v\n", string(dataByte), err)
}

//bench read api
func BenchmarkRead(b *testing.B) {
	//reset timer
	b.ResetTimer()

	//init pond
	p, err := InitPond()
	if err != nil {
		b.Log(err.Error())
		return
	}

	//defer p.Quit()
	for n := 0; n < b.N; n++ {
		dataByte, subErr := readData(p)
		b.Logf("n:%v, read data, data:%v, err:%v\n", n, string(dataByte), subErr)
	}
}