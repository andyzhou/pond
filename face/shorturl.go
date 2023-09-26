package face

import (
	"crypto/md5"
	"errors"
	"fmt"
	"strconv"
)

/*
 * face of short url, implement of `IShortUrl`
 *
 * - gen unique short url
 */

//internal macro define
const (
	CharsetAlphanumeric = iota
	CharsetRandomAlphanumeric
)

//face info
type ShortUrl struct {
	cb func(url, keyword string) bool
}

//construct
func NewShortUrl() *ShortUrl {
	//self init
	self := &ShortUrl{}
	//set default cb
	self.cb = self.defaultCB
	return self
}

//gen short url
func (f *ShortUrl) Generator(url string) (string, error) {
	//use default setup and generate
	return f.GeneratorWithKind(CharsetAlphanumeric, url, f.cb)
}

//gen with assigned kind
func (f *ShortUrl) GeneratorWithKind(
			t int, //charset kind
			url string,
			cb func(url, keyword string) bool,
		) (string, error) {
	var (
		shortUrl string
	)

	//check
	if url == "" || cb == nil {
		return shortUrl, errors.New("invalid parameter")
	}

	//get charset
	charset := f.getCharset(t)
	hexMd5 := fmt.Sprintf("%x", md5.Sum([]byte(url)))
	sections := len(hexMd5)/8

	//generate keyword
	shortUrl = f.generator6(charset, url, hexMd5, sections, cb)
	if shortUrl == "" {
		shortUrl = f.generator8(charset, url, hexMd5, sections, cb)
		if shortUrl == "" {
			return "", errors.New("gen short url failed")
		}
	}
	return shortUrl, nil
}

//////////////
//private func
//////////////

//default cb
func (f *ShortUrl) defaultCB(url, keyword string) bool {
	return true
}

//generate 6 character key
func (f *ShortUrl) generator6(
			charset ,url, hexMd5 string,
			sectionNum int,
			cb func(url, keyword string) bool,
		) string {
	for i := 0; i < sectionNum; i++ {
		sectionHex := hexMd5[i*8:8+i*8]
		bits, _ := strconv.ParseUint(sectionHex, 16, 32)
		bits = bits & 0x3FFFFFFF

		keyword := ""
		for j := 0; j < 6; j++ {
			idx := bits & 0x3D
			keyword = keyword + string(charset[idx])
			bits = bits >> 5
		}

		if cb(url, keyword) {
			return keyword
		}
	}

	return ""
}

//generate 8 character key
func (f *ShortUrl) generator8(
			charset, url,
			hexMd5 string,
			sectionNum int,
			cb func(url, keyword string) bool,
		) string {
	for i := 0; i < sectionNum; i++ {
		sectionHex := hexMd5[i*8:i*8+8]
		bits, _ := strconv.ParseUint(sectionHex, 16, 32)
		bits = bits & 0xFFFFFFFF

		keyword := ""
		for j := 0; j < 8; j++ {
			idx := bits & 0x3D
			keyword = keyword + string(charset[idx])
			bits = bits >> 4
		}

		if cb(url, keyword) {
			return keyword
		}
	}

	return ""
}

//get charset
func (f *ShortUrl) getCharset(t int) string {
	switch t {
	case CharsetAlphanumeric:
		return "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	case CharsetRandomAlphanumeric:
		return "A0a12B3b4CDc56Ede7FGf8Hg9IhJKiLjkMNlmOPnQRopqrSstTuvUVwxWXyYzZ"
	default:
		return ""
	}
}

