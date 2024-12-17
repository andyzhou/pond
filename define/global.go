package define

// sub dir
const (
	SubDirOfSearch = "search"
	SubDirOfFile   = "file"
)

// seconds
const (
	SecondsOfMinute = 60
	SecondsOfHour   = SecondsOfMinute * 60
	SecondsOfDay    = SecondsOfHour * 24
)

// default
const (
	DefaultPacketMaxSize = 2048 //2KB
	DefaultQueueSize     = 1024
)

// others
const (
	RecPerPage           = 10
	ManagerTickerSeconds = 60 //xx seconds
	AsciiCharSize        = 2
)
