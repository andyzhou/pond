package define

// others
const (
	FileErrOfEOF          = "EOF"
	FilePerm              = 0755
	ChunkFileMetaSaveRate = 1 //xx seconds
	ChunkMultiTryTimes    = 3
)

// file para
const (
	ChunkMetaFilePara = "chunk-%v.meta" //chunk meta file
	ChunkDataFilePara = "chunk-%v.data" //chunk data file
	TempZipFilePara   = "%s/%s-%d.zip"  //path/zipFile
)

// data size
const (
	DataSizeOfKB = 1024
	DataSizeOfMB = DataSizeOfKB * 1024
	DataSizeOfGB = DataSizeOfMB * 1000
	DataSizeOfTB = DataSizeOfGB * 1000
)

// default value
const (
	DefaultChunkBlockSize   = 128          //min block data size
	DefaultChunkMaxSize     = DataSizeOfTB //one TB
	DefaultChunkMultiIncr   = 0.1
	DefaultChunkActiveHours = 4 //xx hours
	DefaultChunkMetaTicker  = 5 //xx seconds
)
