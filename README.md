# About
This is a big file storage library, still under developing.

# Config setup
```
//pond config
type Config struct {
    DataPath string //data root path
    ChunkSizeMax int64 //chunk data max size
    ChunkBlockSize int64 //chunk block data size
    FixedBlockSize bool //use fixed block size for data  
    LazyMode bool //switcher for lazy queue opt  
    CheckSame bool //switcher for check same data  
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
go test -bench=Read -benchmem -benchtime=20s

```