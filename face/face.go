package face

import (
	"errors"
	"sync"
)

/*
 * inter face
 */

//global variable
var (
	_face *Face
	_faceOnce sync.Once
)

//face info
type Face struct {
	shortUrl *ShortUrl
	gob *Gob
	zip *Zip
}

//get single instance
func GetFace() *Face {
	_faceOnce.Do(func() {
		_face = NewFace()
	})
	return _face
}

//construct
func NewFace() *Face {
	this := &Face{
		shortUrl: NewShortUrl(),
		gob:  NewGob(),
		zip:  NewZip(),
	}
	return this
}

//quit
func (f *Face) Quit() {
}

//set root path
func (f *Face) SetRootPath(path string) error {
	//check
	if path == "" {
		return errors.New("invalid path parameter")
	}

	//simple setup path
	f.gob.SetRootPath(path)
	f.zip.SetRootPath(path)
	return nil
}

//get relate face
func (f *Face) GetShortUrl() *ShortUrl {
	return f.shortUrl
}
func (f *Face) GetGob() *Gob {
	return f.gob
}
func (f *Face) GetZip() *Zip {
	return f.zip
}
