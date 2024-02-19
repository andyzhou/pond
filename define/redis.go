package define

//default
const (
	DefaultPort = 6379
	DefaultKeyPrefix = "pond_"
	DefaultFileInfoHashKeys = 9
	DefaultFileBaseHashKeys = 5
)

// key pattern
const (
	RedisKeyHashPattern  = "hash:%v"     //*:{kind}
)

//others
const (
	RedisKeyPrefix = "pond_"
)