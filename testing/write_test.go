package testing


import (
	"fmt"
	"github.com/andyzhou/pond"
	"testing"
	"time"
)

/*
 * write testing code
 */

//read data
func writeData(p *pond.Pond) (string, error) {
	//format data
	data := fmt.Sprintf("hell-%v", time.Now().Unix())
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
		shortUrl, subErr := writeData(p)
		b.Logf("n:%v, write data, shortUrl:%v, err:%v\n", n, shortUrl, subErr)
	}
}