# About
This is a big file storage library.

# Feature
- one file support TB level storage
- support redis cache for high performance
- default use beleve search as file info search
- one data root path, one pond storage service

# Config setup
```
//pond config
type Config struct {
    DataPath        string //data root path
	ChunkSizeMax    int64  //chunk data max size
	ChunkBlockSize  int64  //chunk block data size
	FixedBlockSize  bool   //use fixed block size for data
	MinChunkFiles	int    //min chunk files when init
	ReadLazy        bool   //switcher for lazy queue opt
	WriteLazy       bool   //switcher for lazy queue opt
	CheckSame       bool   //switcher for check same data
	FileActiveHours int32  //chunk file active hours
	InterQueueSize  int	   //for inter data save queue size, default 1024
}
```

# How to use?
Please see `example` sub dir.

# Testing
```
cd testing
go test -v
go test -v -run="Read"

go test -bench=.
go test -bench=Write
go test -bench=Read -benchmem -benchtime=20s

```

# Future
- file base and info storage in redis for performance [done]
- add sqlite db for local storage
- add download file data support
- get reuse removed file base info from redis pass lua atomic opt