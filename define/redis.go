package define

//default
const (
	DefaultRedisGroup       = "gen"
	DefaultKeyPrefix        = "pond_"
	DefaultConnTimeOut      = 10 //xx seconds
	DefaultFileInfoHashKeys = 9
	DefaultFileBaseHashKeys = 5
)

//general key
const (
	RedisKeySortedPattern = "pond:%v:%v:sorted" //*:{group}:{tag}:sorted
	RedisKeyHashPattern   = "pond:%v:%v:hash"   //*:{group}:{tag}:hash
)

// key pattern
const (
	RedisKeyFileInfoPattern = "fileInfo:%v" //*:{hashIdx}
	RedisKeyFileBasePattern = "fileBase:%v" //*:{hashIdx}
	RedisKeyFilesList       = "filesList"   //sorted data, shortUrl -> createTime
	RedisKeyRemovedFileBase = "removedBase"
)

//redis key and num info
//used for one node??
const (
	RedisKeyPrefix      = "pond_%v_" //pond_{node}?
	RedisFileInfoKeyNum = 279        //5xBaseKeyNum
	RedisFileBaseKeyNum = 31
)