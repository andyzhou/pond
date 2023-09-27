package util

import (
	"crypto/md5"
	"errors"
	"fmt"
	"github.com/andyzhou/pond/define"
	"os"
)

/*
 * inter utils face
 */

//face info
type Util struct {
}

//md5 sum binary
func (f *Util)  Md5Sum(data []byte) (string, error) {
	//check
	if data == nil || len(data) <= 0 {
		return "", errors.New("invalid parameter")
	}
	//init and sum
	hash := md5.New()
	hash.Write(data)
	val := fmt.Sprintf("%x", hash.Sum(nil))
	return val, nil
}

//check file exists or not
func (f *Util) CheckFile(filePath string) error {
	//check
	if filePath == "" {
		return errors.New("invalid dir parameter")
	}
	_, err := os.Stat(filePath)
	return err
}

//check and make dir
func (f *Util) CheckDir(dir string) error {
	//check
	if dir == "" {
		return errors.New("invalid dir parameter")
	}
	//detect and make dir
	_, err := os.Stat(dir)
	if err != nil {
		//dir not exist
		err = os.Mkdir(dir, define.FilePerm)
		if err != nil {
			return err
		}
	}
	return nil
}

//get current dir
func (f *Util) GetCurDir() (string, error) {
	return os.Getwd()
}
