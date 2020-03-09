package raftkv

const (
	Alive = iota
	Dead

	OK			= "OK"
	ErrNoKey	= "ErrNoKey"
	ErrTimeOut	= "ErrTimeOut"
	ErrWrongOp	= "ErrWrongOp"
)

type Err string

type PutAppendArgs struct {
	Key		string	// Key
	Value	string	// Value
	Op		string	// Put或者Append

	Cid		int64	// 发起的Clerk的id
	Seq		int32	// Seq号
}

type PutAppendReply struct {
	WrongLeader bool	// 是否为错误Leader,Leader会返回false
	Err			Err		// 是否出错
}

type GetArgs struct {
	Key string	// 查询的key
	Cid int64	// Clerk的id
	Seq int32	// seq号
}

type GetReply struct {
	WrongLeader bool	// 是否为错误Leader,Leader会返回false
	Err			Err		// 是否出错
	Value		string	// 对应的value
}
