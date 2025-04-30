package define

// others
const (
	FileErrOfEOF          = "EOF"
	FilePerm              = 0755
	ChunkMultiTryTimes    = 3
)

// file para
const (
	ChunkMetaFilePara = "chunk-%v.meta" //chunk meta file
	ChunkDataFilePara = "chunk-%v.data" //chunk data file
)

// data size
const (
	DataSizeOfKB = 1024 //1kb
	DataSizeOfMB = DataSizeOfKB * 1024
	DataSizeOfGB = DataSizeOfMB * 1000
	DataSizeOfTB = DataSizeOfGB * 1000
)

// default value
const (
	DefaultMinChunkFiles     = 1            //min chunk files
	DefaultChunkBlockSize    = 128          //min block data size
	DefaultChunkMaxSize      = DataSizeOfTB //one TB
	DefaultChunkMultiIncr    = 0.05
	DefaultChunkActiveHours  = 4 //xx hours
	DefaultChunkMetaTicker   = 5 //xx seconds
	DefaultChunkExceedBlocks = 2 //exceed max blocks
)
