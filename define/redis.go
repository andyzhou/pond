package define

//default
const (
	DefaultPort = 6379
	DefaultKeyPrefix = "pond_"
	DefaultConnTimeOut = 10 //xx seconds
	DefaultFileInfoHashKeys = 9
	DefaultFileBaseHashKeys = 5
)

//general key
const (
	RedisKeyHashPattern = "pond:%v:hash"
)

// key pattern
const (
	RedisKeyFileInfoPattern  = "fileInfo:%v"     //*:{hashIdx}
	RedisKeyFileBasePattern  = "fileBase:%v"     //*:{hashIdx}
)

//redis key and num info
//used for one node??
const (
	RedisKeyPrefix = "pond_%v_" //pond_{node}?
	RedisFileInfoKeyNum = 279 //5xBaseKeyNum
	RedisFileBaseKeyNum = 31
)