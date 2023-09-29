package chunk

//inter struct
type (
	//read req
	ReadReq struct {
		Offset int64
		Size int64
		Resp chan ReadResp
	}
	ReadResp struct {
		Data []byte
		Err error
	}

	//write req
	WriteReq struct {
		Data []byte
		Offset int64 //assigned offset for overwrite
		Resp chan WriteResp
	}
	WriteResp struct {
		NewOffSet int64
		BlockSize int64
		Err error
	}
)
