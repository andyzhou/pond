package conf

//pond config
type Config struct {
	DataPath string //data root path
	ChunkSizeMax int64 //chunk data max size
	ChunkBlockSize int64 //chunk block data size
	FixedBlockSize bool //use fixed block size for data
	LazyMode bool //switcher for lazy queue opt
}
