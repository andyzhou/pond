package chunk

//inter struct
type (
	//read req
	ReadReq struct {
		Offset     int64
		End        int64
		SkipHeader bool
	}
	ReadResp struct {
		Data []byte
		Err  error
	}

	//write req
	WriteReq struct {
		Md5    string
		Data   []byte
		Offset int64 //assigned offset for overwrite
	}
	WriteResp struct {
		NewOffSet int64
		BlockSize int64
		Err       error
	}
)
