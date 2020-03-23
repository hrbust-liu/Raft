package raftkv

import (
	"fmt"
	"labrpc"
	"sync/atomic"
	"time"
)
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd	// 每个server的ClientEnd
	lastLeader int	// 上一次发现的Leader编号
	cid int64		// 独一无二的序号
	seq int32		// clerk拥有独立递增的序列号,在Put/Append时递增
}

func nrand() int64{
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x;
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.lastLeader = 0
	ck.cid = nrand()
	return ck
}
// 将Get操作发送给Leader
func (ck *Clerk) Get(key string) string {
	if key == "" {
		return ""
	} else {
		n := len(ck.servers)
		i := 0
		args := GetArgs{key, ck.cid, ck.seq}
		for {	// 不断循环判断Leader
			reply := GetReply{}
			server := (ck.lastLeader + i)%n
			ok := ck.servers[server].Call("KVServer.Get", &args, &reply)
			if ok && reply.WrongLeader == false {	// 说明server是当前Leader
				if reply.Err == OK {	// 说明操作成功
					ck.lastLeader = server
					fmt.Printf("success Get !!! \n")
					return reply.Value
				}
			} else {
				i++
			}
			time.Sleep(100*time.Millisecond)
			fmt.Println("Get fail!! 需要再次提交\n")
		}
	}
}
// 将Put/Append操作发送给Leader
func (ck *Clerk) PutAppend(key string, value string, op string) {
	seq := atomic.AddInt32(&ck.seq, 1)
	n := len(ck.servers)
	i := 0
	args := PutAppendArgs{key, value, op, ck.cid, seq}
	for {
		reply := PutAppendReply{}
		server := (ck.lastLeader+i)%n // 不断寻找当前Leader
		ok := ck.servers[server].Call("KVServer.PutAppend", &args, &reply)
		if ok && reply.WrongLeader == false {	// 说明server是当前Leader
			if reply.Err == OK {	// 说明Append/Put成功
				ck.lastLeader = server
				fmt.Printf("success Put !!! \n")
				return
			}
		} else {
			i++
		}
		time.Sleep(100*time.Millisecond)
		fmt.Println("Put fail!! 需要再次提交\n")
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
