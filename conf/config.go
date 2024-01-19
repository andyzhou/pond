package conf

//pond config
type Config struct {
	DataPath        string //data root path
	ChunkSizeMax    int64  //chunk data max size
	ChunkBlockSize  int64  //chunk block data size
	FixedBlockSize  bool   //use fixed block size for data
	ReadLazy        bool   //switcher for lazy queue opt
	WriteLazy       bool   //switcher for lazy queue opt
	CheckSame       bool   //switcher for check same data
	FileActiveHours int32  //chunk file active hours
}
