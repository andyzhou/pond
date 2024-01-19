package define

import "time"

const (
	ChunksMetaFile     = "chunks.meta"
	ChunksMetaSaveRate = 5 //xx seconds
)

// default value
const (
	MetaAutoSaveTicker = 5 * time.Second
)
