package face

import (
	"archive/zip"
	"bytes"
	"errors"
	"fmt"
	"github.com/andyzhou/pond/define"
	"io"
	"log"
	"os"
	"time"
)

/*
 * zip chunk data face
 * - one chunk one zip file
 * - used for backup chunk data
 */

//face info
type Zip struct {
	rootPath string
}

//construct
func NewZip() *Zip {
	//self init
	this := &Zip{
		rootPath: define.DefaultRootPath,
	}
	return this
}

//pop zip files
func (f *Zip) PopZipFiles(
			zipFileName,
			fileMd5 string,
			cbForWrite func(fileName, fileComment string, fileData []byte) bool,
		) error {
	//check
	if zipFileName == "" || cbForWrite == nil {
		return errors.New("invalid parameter")
	}

	//open zip file
	zipReader, err := f.openZipFileForReader(zipFileName)
	if err != nil {
		return err
	}
	defer zipReader.Close()

	//loop zip sub files
	for _, file := range zipReader.File {
		//check file
		if file.Name != fileMd5 {
			continue
		}
		//try open sub file
		rc, subErr := file.Open()
		if subErr != nil {
			log.Println(err)
		}
		buffer := bytes.NewBuffer(nil)
		_, err = io.Copy(buffer, rc)
		if err != nil {
			log.Println(err)
		}
		//run call back
		if cbForWrite != nil {
			cbForWrite(file.Name, file.Comment, buffer.Bytes())
		}
		rc.Close()
	}
	return nil
}

//add file content into zip
//open old zip and fill into new zip
func (f *Zip) AddContentIntoZip(
			zipFileName string,
			fileName string,
			fileData []byte,
			fileComment string,
		) error {
	//check
	if zipFileName == "" || fileName == "" || fileData == nil {
		return errors.New("invalid parameter")
	}

	//format tmp zip file for new data
	tmpZipFile := fmt.Sprintf(define.TempZipFilePara, f.rootPath, zipFileName, time.Now().Unix())

	//open old zip and copy into temp zip
	zipWriter, err := f.openAndCopyZipFile(zipFileName, tmpZipFile)
	if err != nil {
		return err
	}
	defer zipWriter.Close()

	//add content into new zip
	err = f.addFileContentIntoZip(zipWriter, fileName, fileData, fileComment)
	if err != nil {
		return err
	}

	//rename temp file to original zip name
	os.Remove(zipFileName)
	os.Rename(tmpZipFile, zipFileName)
	return nil
}

//add batch files into zip
func (f *Zip) AddFileIntoZip(
			zipFileName string,
			filesToZip ... string,
		) error {
	//check
	if zipFileName == "" || filesToZip == nil {
		return errors.New("invalid parameter")
	}

	//open zip file
	zipFile, zipWriter, err := f.openZipFileForWriter(zipFileName)
	if err != nil {
		return err
	}

	//defer close
	defer zipFile.Close()
	defer zipWriter.Close()

	//zip batch files
	for _, file := range filesToZip {
		err = f.addFileIntoZip(zipWriter, file)
		if err != nil {
			return err
		}
	}
	return nil
}


//create zip file
func (f *Zip) Create(zipFileName string) error {
	//create zip file
	zipFile, err := os.Create(zipFileName)
	if err != nil {
		return err
	}
	defer zipFile.Close()
	return nil
}

//set root path
func (f *Zip) SetRootPath(path string) error {
	//check
	if path == "" {
		return errors.New("invalid path parameter")
	}
	f.rootPath = path
	return nil
}

////////////////
//private func
////////////////

//add file content into zip
func (f *Zip) addFileContentIntoZip(
			zipWriter *zip.Writer,
			fileName string,
			fileContent []byte,
			fileComment string,
		) error {
	//init file header
	zipHeader := &zip.FileHeader{
		Name:fileName,
		Method:zip.Deflate,
		Comment: fileComment,
	}

	//write zip header
	_, err := zipWriter.CreateHeader(zipHeader)
	if err != nil {
		return err
	}

	//create zip sub file
	file, err := zipWriter.Create(fileName)
	if err != nil {
		return err
	}
	//write file
	_, err = file.Write(fileContent)
	if err != nil {
		return err
	}
	err = zipWriter.Flush()
	return err
}

//add files into zip
func (f *Zip) addFileIntoZip(
			zipWriter *zip.Writer,
			fileName string,
		) error {
	//open file
	fileToZip, err := os.Open(fileName)
	if err != nil {
		return err
	}
	defer fileToZip.Close()

	//check file info
	info, err := fileToZip.Stat()
	if err != nil {
		return err
	}
	header, err := zip.FileInfoHeader(info)
	if err != nil {
		return err
	}

	//setup
	header.Name = fileName
	header.Method = zip.Deflate

	//write header
	writer, err := zipWriter.CreateHeader(header)
	if err != nil {
		return err
	}
	_, err = io.Copy(writer, fileToZip)
	return err
}

//open zip file for read
func (f *Zip) openZipFileForReader(
		zipFileName string) (*zip.ReadCloser, error) {
	//open zip file reader
	zipReader, err := zip.OpenReader(zipFileName)
	return zipReader, err
}

//open zip file for write
func (f *Zip) openZipFileForWriter(
		zipFileName string) (*os.File, *zip.Writer, error) {
	//open file
	zipFile, err := os.Open(zipFileName)
	if err != nil {
		return nil, nil, err
	}
	//create zip file writer
	zipWriter := zip.NewWriter(zipFile)
	return zipFile, zipWriter, err
}

//open old zip and add item into new zip file
//add new files into new zip
func (f *Zip) openAndCopyZipFile(
		oldZipFile, targetZipFile string) (*zip.Writer, error) {
	//create zip writer
	targetFile, err := os.Create(targetZipFile)
	if err != nil {
		return nil, err
	}
	targetZipWriter := zip.NewWriter(targetFile)

	//copy old zip file
	zipReader, err := zip.OpenReader(oldZipFile)
	if err == nil {
		for _, zipItem := range zipReader.File {
			zipItemReader, _ := zipItem.Open()
			header, _ := zip.FileInfoHeader(zipItem.FileInfo())
			targetItem, _ := targetZipWriter.CreateHeader(header)
			io.Copy(targetItem, zipItemReader)
		}
	}
	return targetZipWriter, nil
}