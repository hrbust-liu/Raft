package raftkv

import (
	"bytes"
	"labgob"
	"labrpc"
	"log"
	"raft"
	"sync"
	"time"
)

const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {	// 传递的命令,日志保存的command
	Key string		// key
	Value string	// value
	Type string		// 类型 Put/Append

	Cid int64		// 请求的clerk id
	Seq int32		// seq
}

func (a *Op) equals(b Op) bool {
	return a.Seq == b.Seq && a.Cid == b.Cid && a.Value == b.Value && a.Key == b.Key && a.Type == b.Type
}
type KVServer struct {
	mu		sync.Mutex
	me		int			// 自己编号
	rf		*raft.Raft	// raft
	applyCh	chan raft.ApplyMsg	// 获取commit消息

	maxraftstate	int

	keyValue map[string] string	// 记录key/value对
	getCh map[int]chan Op		// 为每条命令创建通道，该命令完成commit以后，server收到通知, 并返回给clerk
	cid_seq map[int64] int32	// 记录这个clerk id提交最大写seq
	persister *raft.Persister

	stat int
}
// 返回getCh[index],没有则创建
func (kv *KVServer) getAgreeCh(index int) chan Op{
	kv.mu.Lock()
	ch, ok := kv.getCh[index]
	if !ok {
		ch = make(chan Op, 1)
		kv.getCh[index] = ch
	}
	kv.mu.Unlock()
	return ch
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	reply.WrongLeader = false
	seq, ok := kv.cid_seq[args.Cid]
	if ok && seq > args.Seq {	// 最新的数据都已经写进来了,因此你这个请求我可以完成
		kv.mu.Lock()
		reply.Value = kv.keyValue[args.Key]
		reply.Err = OK
		kv.mu.Unlock()
		return
	}
	cmd := Op{args.Key, "", "Get", args.Cid, args.Seq}
	index,_,isLeader := kv.rf.Start(cmd)	// 抛出请求,并判断该节点是否为Leader
	if !isLeader {	// 该节点不是Leader
		reply.WrongLeader = true
		return
	} else {
		ch := kv.getAgreeCh(index)
		op := Op{}
		DPrintf("select ")
		select {	// 等待返回,当commit以后,便会返回
		case op = <-ch:
			reply.Err = OK
			DPrintf("%v agree\n", kv.me)
			close(ch)
		case <-time.After(1000*time.Millisecond):
			reply.Err = ErrTimeOut
			reply.WrongLeader = true
			DPrintf("timeout\n")
			return
		}
		DPrintf("success\n")
		if !cmd.equals(op) {
			reply.Err = ErrWrongOp
			reply.WrongLeader = true
			return
		}
		kv.mu.Lock()
		reply.Value = kv.keyValue[args.Key]
		kv.mu.Unlock()
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	cmd := Op{args.Key, args.Value, args.Op, args.Cid, args.Seq}
	index, _, isLeader := kv.rf.Start(cmd)
	reply.WrongLeader = false
	if !isLeader {
		reply.WrongLeader = true
		return
	} else {
		ch := kv.getAgreeCh(index)
		op := Op{}
		DPrintf("select ")
		select {	// 等待返回或者超时,会在commit以后返回
		case op = <-ch:
			reply.Err = OK
			DPrintf("%v agree\n", kv.me)
			close(ch)
		case <-time.After(1000*time.Millisecond):
			reply.Err = ErrTimeOut
			reply.WrongLeader = true
			DPrintf("timeout\n")
			return
		}
		DPrintf("success\n")
		if !cmd.equals(op) {
			reply.Err = ErrWrongOp
			reply.WrongLeader = true
			return
		}
	}
}

func (kv *KVServer) Kill() {
	kv.rf.Kill()
	kv.mu.Lock()
	kv.stat = Dead
	kv.mu.Unlock()
	DPrintf("%v is dead\n", kv.me)
}
// 检查是否空间占用过大,需要做快照
func (kv *KVServer) checkSnapShot(index int) {
	if kv.maxraftstate == -1 {
		return
	}
	if kv.persister.RaftStateSize() < kv.maxraftstate*9/10{
		return
	}
	go kv.rf.TakeSnapShot(index, kv.cid_seq, kv.keyValue)
}
// 等待提交消息
func (kv *KVServer) waitSubmitLoop() {
	for {
		kv.mu.Lock()
		stat := kv.stat
		kv.mu.Unlock()
		if stat == Dead {
			return
		}
		select {
		case msg := <-kv.applyCh:	// 获取commit消息
			if msg.CommandValid {
				op := msg.Command.(Op)
				kv.mu.Lock()
				maxSeq, ok := kv.cid_seq[op.Cid]
				if !ok || op.Seq >maxSeq{	// 如果该cid的最大seq没超过本个,则更新
					switch op.Type {
					case "Put":
						kv.keyValue[op.Key] = op.Value
					case "Append":
						kv.keyValue[op.Key] += op.Value
					}
					kv.cid_seq[op.Cid] = op.Seq
				}
				kv.mu.Unlock()
				kv.checkSnapShot(msg.CommandIndex)
				kv.getAgreeCh(msg.CommandIndex) <- op
			} else {
				kv.readSnapShot(msg.Data)
			}
		}
	}
}
// 读取快照,进行解析
func (kv *KVServer) readSnapShot(data []byte) {
	if data== nil || len(data) < 1 {
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var cid_seq map[int64] int32
	var keyValue map[string] string

	err := d.Decode(&cid_seq)
	if err == nil{
		kv.cid_seq = cid_seq
	} else {
		log.Fatal("decode cid_seq error:", err)
	}

	err = d.Decode(&keyValue)
	if err == nil{
		kv.keyValue = keyValue
	} else {
		log.Fatal("decode keyValue err:", err)
	}

}
// 启动KVServer
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.getCh = make(map[int]chan Op)
	kv.keyValue = make(map[string] string)
	kv.cid_seq = make(map[int64] int32)
	kv.stat = Alive
	kv.persister = persister
	kv.readSnapShot(persister.ReadSnapshot())

	go kv.waitSubmitLoop()

	return kv
}
