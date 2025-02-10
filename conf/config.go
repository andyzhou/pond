package conf

//pond base config
type Config struct {
	DataPath        string //data root path
	ChunkSizeMax    int64  //chunk data max size
	ChunkBlockSize  int64  //chunk block data size
	FixedBlockSize  bool   //use fixed block size for data
	MinChunkFiles	int    //min chunk files when init
	ReadLazy        bool   //switcher for lazy queue opt
	WriteLazy       bool   //switcher for lazy queue opt
	CheckSame       bool   //switcher for check same data
	UseMemoryMap	bool   //switcher for use memory map file
	FileActiveHours int32  //chunk file active hours
	InterQueueSize  int	   //for inter data save queue size, default 1024
}

//redis config (optional)
//used for file meta info storage
type RedisConfig struct {
	//basic
	GroupTag string //used for group keys
	Address  string
	Password string
	DBNum    int
	Pools    int

	//optional
	KeyPrefix        string
	FileInfoHashKeys int
	FileBaseHashKeys int
}
