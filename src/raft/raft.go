package raft

import "labrpc"
import "labgob"

import "bytes"
import "fmt"
import "log"
import "math/rand"
import "sync"
import "time"

// 节点状态
const (
	Follower = iota
	Candidate
	Leader
	Dead
)
// 发送的数据
type ApplyMsg struct {
	CommandValid bool	// 数据是否有效
	Command interface{}	// 真正的命令,(kvraft中的Op)
	CommandIndex int
	Data	[]byte
}

type Raft struct {
	mu			sync.Mutex
	peers		[]*labrpc.ClientEnd	// 所有节点
	persister	*Persister			// 用于持久化,序列化
	me			int					// 自己序号

	timer*		time.Timer			// 定时器,timer.C会进行通知
	stat		int					// 状态

	applyCh		chan	ApplyMsg	// commit消息放入该通道
	appendCh	chan	struct{}
	voteCh		chan	struct{}	// 等待投票管道
	commitCh	chan	struct{}	// 等待commit管道
	voteCount	int					// 获得读票数

	currentTerm	int		// 当期term
	voteFor		int		// -1代表未投票
	log			[]Entry	// 当期记录的日志

	commitIndex	int		// 被节点共识,可以提交的数据索引
	lastApplied	int		// 真正保存到本机的索引
	nextIndex	[]int	// 准备发送的数据
	matchIndex	[]int	// 已经确认的数据
}
type Entry struct {
	Term	int			// 提交时的term
	Index	int			// 本个数据读序号
	Command	interface{}	// 数据内容,(kvraft中的Op)
	Commited bool
}

func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	term =rf.currentTerm
	isleader = (rf.stat == Leader)
	return term, isleader
}

func min(x int, y int) int{
	if x < y{
		return x
	} else {
		return y
	}
}

func max(x int, y int) int{
	if x > y {
		return x
	} else {
		return y
	}
}
// 	设置超时时间:超过本次时间成为候选人
func getRandomTimeOut() time.Duration{
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	timeout := time.Millisecond*time.Duration(r.Int63n(500)+250)
	return timeout
}
// 将term, voteFor, log进行编码
func (rf *Raft) getPersistData() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	err := e.Encode(rf.currentTerm)
	if err != nil {
		log.Fatal("encode term error:", err)
	}
	err = e.Encode(rf.voteFor)
	if err != nil {
		log.Fatal("encode voteFor error:", err)
	}
	err = e.Encode(rf.log)
	if err != nil {
		log.Fatal("encode log error:", err)
	}
	data := w.Bytes()
	return data
}
func (rf *Raft) persist(){
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	err := e.Encode(rf.currentTerm)
	err = e.Encode(rf.voteFor)
	err = e.Encode(rf.log)

	if err != nil {
		log.Fatal("encode term error:",err);
	}

	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var term int
	var voteFor int
	var entries []Entry
	err := d.Decode(&term)
	if err != nil {
		log.Fatal("decode term error:", err)
	} else {
		rf.currentTerm = term
	}
	err = d.Decode(&voteFor)
	if err != nil {
		log.Fatal("decode voteFor error:", err)
	} else {
		rf.voteFor = voteFor
	}
	err = d.Decode(&entries)
	if err != nil {
		log.Fatal("decode log error:", err)
	} else {
		rf.log = entries
	}
}
// 保存(State = (term, voteFor, log) , snapshotData = (cis_seq, kv))其中kv记录index之前的数据,之后的仍然用log记录
func (rf *Raft) TakeSnapShot(index int, cid_seq map[int64]int32, kv map[string]string) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	FirstIndex := rf.log[0].Index
	if index < FirstIndex {
		fmt.Printf("index:%v, FirstIndex:%v\n", index, FirstIndex)
		return
	}
	rf.log = rf.log[index - FirstIndex:]

	w2 := new(bytes.Buffer)
	e2 := labgob.NewEncoder(w2)
	err := e2.Encode(cid_seq)
	if err != nil {
		log.Fatal("encode KVServer's data error:", err)
	}
	err = e2.Encode(kv)
	if err != nil {
		log.Fatal("encode KVServer's data error:", err)
	}
	snapshotsData := w2.Bytes()
	rf.persister.SaveStateAndSnapshot(rf.getPersistData(), snapshotsData)
}
// 发送数据/作为心跳时 发送参数
type AppendEntriesArgs struct {
	Term int			// term
	LeaderId int		// 我的id
	PrevLogIndex int	// 上一个index是多少  只有这两个参数都一样时，才可以接受本数据
	PrevLogTerm int		// 上一个term时多少 只有这两个参数都一样时，才可以接受本数据
	Entries []Entry		// 数据 即log
	LeaderCommit int	// 可以commit的编号
}
// 返回参数
type AppendEntriesReply struct {
	Term int			// 当期iterm
	Success bool		// 是否PrevLogIndex与PrevLogTerm一致，不一致代表需要中间部分数据
	PrevIndex int		// 所需最小的log索引 
	LastLogIndex int
}
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	reply.Success = false
	if rf.stat == Dead{
		return
	}
	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	} else if args.Term > rf.currentTerm && rf.stat != Dead{
		rf.mu.Lock()
		rf.currentTerm = args.Term
		rf.stat = Follower
		rf.voteFor = -1
		rf.mu.Unlock()
		rf.persist()
	}
	if args.Term >= rf.currentTerm {
		// 由于log不断增加，因此之前部分log可能会删除，log[0]的index可能不一样，PrevIndex则是当前raft下的偏移
		FirstIndex := rf.log[0].Index
		PrevIndex := args.PrevLogIndex - FirstIndex
		if PrevIndex < 0 {
			reply.PrevIndex = FirstIndex
			return
		}
		if args.PrevLogIndex != -1 && args.PrevLogTerm != -1 {
			if len(rf.log) - 1 < PrevIndex || rf.log[PrevIndex].Term != args.PrevLogTerm {
				reply.Success = false
				if len(rf.log) - 1 >= PrevIndex {
					tmp := PrevIndex
					// 之前Leader,存储了垃圾log
					for tmp > 0 && rf.log[tmp].Term == rf.log[PrevIndex].Term {
						tmp--
					}
					reply.PrevIndex = rf.log[tmp].Index
				} else {
					reply.PrevIndex = rf.log[len(rf.log) - 1].Index
				}
			} else if rf.stat == Follower && rf.commitIndex <= args.LeaderCommit{
				rf.mu.Lock()
				reply.Success = true
				j := 0
				if len(rf.log) > PrevIndex {	// 将相同index但iterm不同于Leader的log删除
					for i := PrevIndex + 1; i <len(rf.log) && j <len(args.Entries); i++{
						if rf.log[i].Index == args.Entries[j].Index && rf.log[i].Term != args.Entries[j].Term {
							rf.log = rf.log[:i]
							break
						}
						j++
					}
				}
				// j之后则是本节点既不存在，又需要保存的log
				rf.log = append(rf.log, args.Entries[j:]...)

				rf.persist()
				// 有新的需要commit的
				if args.LeaderCommit > rf.commitIndex {
					if (len(args.Entries) > 0){
						rf.commitIndex = min(args.LeaderCommit, args.Entries[len(args.Entries) -1].Index)
					} else {
						rf.commitIndex = args.LeaderCommit
					}
					rf.commitCh <- struct{}{}
				}
				rf.mu.Unlock()
			}
		}
	}
	reply.Term = rf.currentTerm
	reply.LastLogIndex = len(rf.log) - 1

	if reply.Success {
		go func() {
			rf.appendCh <- struct{}{}
		}()
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}
// 请求投票
type RequestVoteArgs struct {
	Term int			// term
	CandidateId int		// 请求节点 Id
	LastLogIndex int	// 请求节点最后日志的 index
	LastLogTerm int		// 请求节点最后日志的 term
}
type RequestVoteReply struct {
	Term int			// 本节点目前term
	VoteGranted bool	// 是否同意
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.VoteGranted = false
	//fmt.Printf("I am %v, I get %v requeset, args.Term = %v, My term = %v\n",rf.me,args.CandidateId,args.Term,rf.currentTerm)
	if args.Term>rf.currentTerm{
		rf.currentTerm = args.Term
		rf.stat = Follower
		rf.voteFor = -1
		rf.persist()
	}
	if args.Term >= rf.currentTerm && (rf.voteFor == -1 || rf.voteFor == args.CandidateId){
		lastLogIndex := len(rf.log) - 1
		lastLogTerm := rf.log[len(rf.log)-1].Term
		// arg日志更多，则投票
		if (lastLogTerm < args.LastLogTerm) || (lastLogTerm == args.LastLogTerm && lastLogIndex <= args.LastLogIndex) {
			reply.VoteGranted = true
			rf.voteFor = args.CandidateId
			rf.persist()
		}
	}
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm{
		reply.VoteGranted = false
	} else {
		go func() {
			rf.voteCh <- struct{}{}
		}()
	}
}

func (rf *Raft) updateStatTo(stat int) {
	if rf.stat == stat || rf.stat == Dead{
		return
	}
	if stat == Follower {
		rf.stat = Follower
		rf.voteFor = -1
		fmt.Printf("%v update to Follower\n",rf.me)
	}
	if stat == Candidate {
		rf.mu.Lock()
		rf.stat = Candidate
		rf.mu.Unlock()
		fmt.Printf("%v update to Candidate\n",rf.me)
		rf.startElection()	// 准备选举
	}
	if stat == Leader {
		fmt.Printf("%v update to Leader\n",rf.me)
		rf.stat = Leader
		rf.nextIndex = make([]int, len(rf.peers))
		rf.matchIndex = make([]int, len(rf.peers))
		for i, _ :=range rf.peers{
			if i != rf.me {
				rf.nextIndex[i] = rf.log[len(rf.log)-1].Index + 1
				rf.matchIndex[i] = 0
			}
		}
	}
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	term = rf.currentTerm
	isLeader = (rf.stat == Leader)
	if isLeader{
		index = rf.log[len(rf.log)-1].Index+1
		entry := Entry{term, index, command, false}
		rf.log=append(rf.log, entry)
		rf.persist()
	}
	return index, term, isLeader
}

func (rf *Raft) Kill() {
	rf.mu.Lock()
	rf.stat = Dead
	rf.mu.Unlock()
}
// 成为候选人之后，试图进行选举
func (rf *Raft) startElection() {
	if rf.stat == Dead{
		return
	}
	rf.mu.Lock()
	rf.voteFor = rf.me
	rf.currentTerm += 1
	rf.mu.Unlock()

	rf.voteCount = 1
	rf.timer.Reset(getRandomTimeOut())
	rf.persist()

	args := RequestVoteArgs{}
	args.Term = rf.currentTerm
	args.CandidateId = rf.me
	if len(rf.log) > 0 {
		args.LastLogIndex = len(rf.log) - 1
		args.LastLogTerm = rf.log[args.LastLogIndex].Term
	}

	for i, _:=range rf.peers {
		if i != rf.me {
			go func(server int) {
				//fmt.Printf("I am %v, please %v give me vote\n",rf.me, server)
				reply := RequestVoteReply{}
				ok := rf.sendRequestVote(server, &args, &reply)
				//fmt.Printf("I am %v , %v say: ok = %v reply = %v\n",rf.me, server,ok,reply)
				if rf.stat == Candidate && ok && reply.Term == rf.currentTerm{
					if reply.VoteGranted{
						rf.mu.Lock()
						rf.voteCount += 1
						rf.mu.Unlock()
					} else if reply.Term > rf.currentTerm {
						// TODO
						rf.mu.Lock()
						rf.currentTerm = reply.Term
						rf.stat = Follower
						rf.voteFor = -1
						rf.mu.Unlock()
						rf.persist()
					}
				}else {
				}
				//fmt.Printf("I am %v, now voteCount=%v, I Need %v\n",rf.me,rf.voteCount,len(rf.peers)/2+1)
			}(i)
		}
	}
}
// 为每个节点追加log
func (rf *Raft) broadcastAppendEntriesRPC() {
	for i, _ :=range rf.peers {
		FirstIndex := rf.log[0].Index
		if i != rf.me {
			go func(server int) {
				args := AppendEntriesArgs{}
				reply := AppendEntriesReply{}
				args.Term = rf.currentTerm
				args.LeaderId = rf.me
				args.LeaderCommit = rf.commitIndex
				args.PrevLogIndex = -1
				args.PrevLogTerm = -1

				// TODO 如果我第一个log之前的数据也有缺少,需要将目前快照发送过去
				if rf.nextIndex[server] > FirstIndex {
					index := rf.matchIndex[server]
					if len(rf.log) > 0 && len(rf.log) > index {
						args.PrevLogIndex = index-FirstIndex
						args.PrevLogTerm = rf.log[index-FirstIndex].Term
						args.Entries = rf.log[index+1-FirstIndex:]
					}
				}
				// 将match之后的数据都发送过去
				ok := rf.sendAppendEntries(server, &args, &reply)
				ok = ok && reply.Term == rf.currentTerm
				if reply.Term > rf.currentTerm {
					rf.mu.Lock()
					rf.currentTerm = reply.Term
					rf.stat = Follower
					rf.voteFor = -1
					rf.mu.Unlock()
					rf.persist()
				}
				if ok && rf.stat == Leader {
					if len(rf.log) > 0 && len(args.Entries) >0{
						rf.nextIndex[server] = args.Entries[len(args.Entries) - 1].Index + 1
						rf.matchIndex[server] = rf.nextIndex[server] - 1
					}
					if rf.commitIndex < rf.matchIndex[server] {
						rf.updateCommitIndex()
						if rf.commitIndex <= reply.LastLogIndex {
							rf.commitCh <- struct{}{}
						}
					}
				}else if rf.stat==Leader{
					if rf.nextIndex[server] > reply.PrevIndex + 1{
						rf.nextIndex[server] = reply.PrevIndex + 1
					} else {
						rf.nextIndex[server] = max(rf.nextIndex[server] -1, 1)
					}
				}
			}(i)
		}
	}
}
// 监听commit
func (rf *Raft) doCommitLoop() {
	for {
		if rf.stat == Dead {
			return
		}
		select {
			// 等待commit信号
			case <- rf.commitCh:
			for {
				if rf.commitIndex > rf.lastApplied {
					FirstIndex := rf.log[0].Index
					if rf.lastApplied + 1 >= FirstIndex {
						rf.mu.Lock()
						rf.lastApplied += 1
						rf.mu.Unlock()
						index := rf.lastApplied - FirstIndex

						// 真正在本节点提交数据
						msg := ApplyMsg{}
						msg.Command = rf.log[index].Command
						msg.CommandIndex = rf.lastApplied
						msg.CommandValid = true
						rf.applyCh <- msg
						rf.log[index].Commited = true
					}
				} else {
					break
				}
			}
		}
	}
}
// 更新commit
func (rf *Raft) updateCommitIndex() {
	N := rf.commitIndex
	FirstIndex := rf.log[0].Index
	// 该数据所在节点数量过半,则可以提交
	for i := max(rf.commitIndex + 1, FirstIndex + 1); i <= rf.log[len(rf.log) - 1].Index; i++ {
		cnt := 1
		for j := range rf.peers {
			if j != rf.me {
				if rf.matchIndex[j] >= i && rf.log[i - FirstIndex].Term == rf.currentTerm{
					cnt++
				}
			}
		}
		if cnt > len(rf.peers)/2 {
			N = i
		}
	}
	if N > rf.commitIndex && rf.stat == Leader {
		rf.mu.Lock()
		rf.commitIndex = N
		rf.mu.Unlock()
	}
}
// 无限循环在Follower/Candidate/Leader
func (rf *Raft) mainLoop() {
	for {
		switch rf.stat{
		case Follower:
			// 追加数据,或者请求投票会让时间重置,超时则成为候选人
			select {
			case <- rf.appendCh:
				rf.timer.Reset(getRandomTimeOut())
			case <-rf.voteCh:
				rf.timer.Reset(getRandomTimeOut())
			case <-rf.timer.C:
				rf.updateStatTo(Candidate)
			}
		case Candidate:
			// 收到数据说明有Leader,否则请求投票,票数过半则成为Leader
			select {
			case <- rf.appendCh:
				rf.timer.Reset(getRandomTimeOut())
				rf.mu.Lock()
				rf.stat = Follower
				rf.voteFor = -1
				rf.mu.Unlock()
				rf.persist()
			case <-rf.timer.C:
				rf.startElection()
			default:
				if rf.voteCount > len(rf.peers)/2 {
					rf.mu.Lock()
					rf.updateStatTo(Leader)
					rf.mu.Unlock()
				}
			}
		case Leader:
			rf.broadcastAppendEntriesRPC()
			time.Sleep(50*time.Millisecond)
		case Dead:
			return
		}
	}
}

// 初始化生成Raft
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
		rf := &Raft{}
		rf.peers = peers
		rf.persister = persister
		rf.me = me

		rf.stat = Follower
		rf.currentTerm = 0
		rf.voteFor = -1
		rf.commitIndex = 0
		rf.lastApplied = 0
		rf.log = make([]Entry, 0)
		rf.log = append(rf.log, Entry{0, 0, 0, true})
		rf.voteCh = make(chan struct{})
		rf.appendCh = make(chan struct{})
		rf.commitCh = make(chan struct{})
		rf.applyCh = applyCh

		rf.timer = time.NewTimer(getRandomTimeOut())
		go rf.mainLoop()
		go rf.doCommitLoop()
		rf.readPersist(persister.ReadRaftState())

		return rf
}
