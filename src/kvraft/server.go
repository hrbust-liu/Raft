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
	applyCh	chan raft.ApplyMsg	// 获取commit消息,当某个command被commit会将该cmd放到到该通道

	maxraftstate	int			// 最多可以保存多少log,当超过本阈值90%就要进行快照处理

	keyValue map[string] string	// 记录key/value对
	getCh map[int]chan Op		// 为每条命令创建通道，该命令完成commit以后，server收到通知, 并返回给clerk
	cid_seq map[int64] int32	// 记录这个clerk id在本server/raft已经commit并执行完的seq数(因为有些数据虽然commit但在本节点仍未commit)
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

// 对于Get，如果本节点更新足够快，使得该clerk下的操作已经更新到Get请求之后，那么可以进行提供本次请求
// 先将请求组装为cmd, 扔到raft中，获得唯一的操作序号index(如果本个节点不是Leader会返回)
// 通过index获取全局唯一的通道,在通道等待返回结果, 判断返回的操作是否为本次操作，若不是说明执行了错误的操作
// 由于是Get操作，此时才可以进行读取
func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	reply.WrongLeader = false
	kv.mu.Lock()
	seq, ok := kv.cid_seq[args.Cid]
	kv.mu.Unlock()
	if ok && seq > args.Seq {	// 该clerk下更新的数据都已经commit并执行完,因此你这个请求我可以完成
		DPrintf("Get 应该没问题 my id = %v\n",kv.me)
		reply.Value = kv.keyValue[args.Key]
		reply.Err = OK
		return
	}
	cmd := Op{args.Key, "", "Get", args.Cid, args.Seq}
	index,_,isLeader := kv.rf.Start(cmd)	// 抛出请求,并判断该节点是否为Leader, 返回一个全局唯一的序号,最为本次提交的index
	DPrintf("分配了 Get index = %v", index)
	if !isLeader {	// 该节点不是Leader
		reply.WrongLeader = true
		return
	} else {
		ch := kv.getAgreeCh(index)			// index是唯一的,可以获取唯一的通道
		op := Op{}
		select {	// 等待返回,当commit以后,便会返回
		case op = <-ch:
			reply.Err = OK
			DPrintf("success Get %v agree, seq = %v, index = %v\n", kv.me, op.Seq, index)
			close(ch)
		case <-time.After(1000*time.Millisecond):
			reply.Err = ErrTimeOut
			reply.WrongLeader = true
			DPrintf("Get timeout\n")
			return
		}
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

// 与Get类似,但操作必须在Leader下，先将请求组装为cmd, 扔到raft中，获得唯一的操作序号index(如果本个节点不是Leader会返回)
// 通过index获取全局唯一的通道,在通道等待返回结果, 判断返回的操作是否为本次操作，若不是说明执行了错误的操作
func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	cmd := Op{args.Key, args.Value, args.Op, args.Cid, args.Seq}
	index, _, isLeader := kv.rf.Start(cmd)
	DPrintf("分配了 Put index = %v", index)
	reply.WrongLeader = false
	if !isLeader {
		reply.WrongLeader = true
		return
	} else {
		ch := kv.getAgreeCh(index)
		op := Op{}
		select {
		case op = <-ch:
			reply.Err = OK
			DPrintf("success Put/Append %v agree, index = %v, key = %v, value = %v\n", kv.me, index, op.Key, op.Value)
			close(ch)
		case <-time.After(1000*time.Millisecond):
			reply.Err = ErrTimeOut
			reply.WrongLeader = true
			DPrintf("Put timeout\n")
			return
		}
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
	DPrintf("%v 检查空间 RaftStateSize() = %v maxraftstate = %v\n", kv.me,  kv.persister.RaftStateSize(), kv.maxraftstate)
	if kv.maxraftstate == -1 {
		return
	}
	if kv.persister.RaftStateSize() < kv.maxraftstate*9/10{
		return
	}
	// 不加锁可能导致有新操作到kv上，而log却没有以为某些操作没有做，从而多做了一次
	go kv.rf.TakeSnapShot(index, kv.cid_seq, kv.keyValue)
}
// 等待提交消息
func (kv *KVServer) waitSubmitLoop() {
	for {
		if kv.stat == Dead {
			return
		}
		select {
		case msg := <-kv.applyCh:	// 获取commit消息,kv存储的数据是一直到commit的
			if msg.CommandValid {
				op := msg.Command.(Op)
				kv.mu.Lock()
				maxSeq, ok := kv.cid_seq[op.Cid]
				if !ok || op.Seq >maxSeq {	// 如果该cid还没更新到本个seq，则进行更新(因为commit在多个raft节点,因此同一个cmd可能会收到多次msg)
					switch op.Type {		// 本个server可以执行该操作
					case "Put":
						kv.keyValue[op.Key] = op.Value
					case "Append":
						kv.keyValue[op.Key] += op.Value
					DPrintf("%v update key = %v, value = %v client = %v\n", kv.me, op.Key, kv.keyValue[op.Key],op.Cid)
					}
					kv.cid_seq[op.Cid] = op.Seq
				}
				kv.mu.Unlock()
				kv.checkSnapShot(msg.CommandIndex)
				kv.getAgreeCh(msg.CommandIndex) <- op	// 按理说只有Leader需要
			} else {	// 当server消息差太多,Leader会直接发送快照，server收到快照后直接把快照数据发送过来
				kv.readSnapShot(msg.Data)
			}
		}
	}
}
// 读取快照,进行解析, 获取cid_seq, kv
func (kv *KVServer) readSnapShot(data []byte) {
	if data== nil || len(data) < 1 {
		return
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var cid_seq map[int64] int32
	var keyValue map[string] string

	if d.Decode(&cid_seq) != nil ||
		d.Decode(&keyValue) != nil {
		log.Fatal("readSnapShot decode err:")
	} else {
		kv.cid_seq = cid_seq
		kv.keyValue = keyValue
	}
}
// 启动KVServer, servers为连接所有servers的端口(ends), 需要生成自己的raft节点
// (server与raft一一对应，raft负责自动选主，同时只有raft-Leader节点可以接受写数据,并负责将数据发送给其他节点
// 因此server节点能否提供服务，取决于该raft节点是否为Leader
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)		// 生成raft

	kv.getCh = make(map[int]chan Op)
	kv.keyValue = make(map[string] string)
	kv.cid_seq = make(map[int64] int32)
	kv.stat = Alive
	kv.persister = persister
	kv.readSnapShot(persister.ReadSnapshot())
	DPrintf("StartKVServer %v keyValue = %v\n", me, kv.keyValue)

	go kv.waitSubmitLoop()

	return kv
}
