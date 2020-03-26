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
	Command interface{}	// 真正的命令, (kvraft中的Op)
	CommandIndex int	// index序号
	Data	[]byte		// 有时数据差太多，读取快照后，直接返回server一个快照
}

type Raft struct {
	mu			sync.Mutex
	peers		[]*labrpc.ClientEnd	// 所有节点
	persister	*Persister			// 用于持久化,序列化
	me			int					// 自己序号

	timer*		time.Timer			// 定时器,利用timer.C会进行通知超时
	stat		int					// 状态

	applyCh		chan	ApplyMsg	// commit消息放入该通道,传给server
	appendCh	chan	struct{}
	voteCh		chan	struct{}	// 等待投票管道
	commitCh	chan	struct{}	// 等待commit管道,Leader率先发起commit,通过AppendEntries让每个节点进行commit
	voteCount	int					// 获得的票数

	currentTerm	int		// 当期term
	voteFor		int		// -1代表未投票
	log			[]Entry	// 当期记录的日志

	commitIndex	int		// 被节点共识,可以提交的数据索引
	lastApplied	int		// 本节点成功完成的序号，一定小于等于commit
	nextIndex	[]int	// 猜测需要发送的数据
	matchIndex	[]int	// 已经确认发送的数据，只能通过已经确定发送的来改变commitIndex

	snapshottedIndex  int // 记录快照最后一个index
}
type Entry struct {
	Term	int			// 提交时的term
	Index	int			// 本个数据的序号
	Command	interface{}	// 数据内容,(kvraft中的Op)
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

	if e.Encode(rf.currentTerm) != nil ||
		e.Encode(rf.voteFor) != nil ||
		e.Encode(rf.log) != nil ||
		e.Encode(rf.commitIndex) != nil ||
		e.Encode(rf.snapshottedIndex) != nil {
			log.Fatal("getPersistData error!!\n")
	}
	data := w.Bytes()
	return data
}
func (rf *Raft) persist(){
	data := rf.getPersistData()
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
	var commitIndex int
	var snapshottedIndex int
	if d.Decode(&term) != nil ||
		d.Decode(&voteFor) != nil ||
		d.Decode(&entries) != nil ||
		d.Decode(&commitIndex) != nil ||
		d.Decode(&snapshottedIndex) != nil {
			log.Fatal("read Persist error!!\n");
	} else {
		rf.currentTerm = term
		rf.voteFor = voteFor
		rf.log = entries
		rf.commitIndex = commitIndex
		rf.lastApplied= snapshottedIndex
		rf.snapshottedIndex = snapshottedIndex
	}
}
// 进行快照将Index之前的保存(State = (term, voteFor, log) , snapshotData = (cis_seq, kv))其中kv记录index之前的数据,之后的仍然用log记录
func (rf *Raft) TakeSnapShot(index int, cid_seq map[int64]int32, kv map[string]string) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	fmt.Printf("me = %v Ready TakeSnapshot!!\n",rf.me)

	FirstIndex := rf.log[0].Index
	if index < FirstIndex {
		fmt.Printf("TakeSnapshot index < FirstIndex\n")
		return
	}
	rf.snapshottedIndex = index
	fmt.Printf("rf.me = %v TakeSnapshot snapshottedIndex = %v\n", rf.me, rf.snapshottedIndex)
	rf.log = rf.log[index - FirstIndex:]	// 给log日志留一个，否则不好操作

	w2 := new(bytes.Buffer)
	e2 := labgob.NewEncoder(w2)
	if e2.Encode(cid_seq) != nil ||
		e2.Encode(kv) != nil {
			log.Fatal("TakeSnapshot error !!\n")
	}
	snapshotsData := w2.Bytes()
	rf.persister.SaveStateAndSnapshot(rf.getPersistData(), snapshotsData)
	fmt.Printf("me = %v TakeSnapshot OK!!\n")
}
type InstallSnapshotArgs struct {
	Term int				// 当前term
	LeaderId int			// 当前leader
	LeaderCommit int		// 当前leader最后一次commit
	LastIncludedIndex int	// 快照最后一个数据的index
	LastIncludedTerm int	// 快照最后一个数据的term
	Data []byte				// snapshot
	Entries []Entry			// snapshot后的log
}
type InstallSnapshotReply struct {
	Term int
	Success bool
	PrevIndex int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply)  {
	if rf.stat == Dead {
		return
	}
	fmt.Printf("%v ready to install snapshot\n", rf.me)

	reply.Success = false
	if args.Term < rf.currentTerm {	// 说明发送快照的leader还没我的term大
		reply.Term = rf.currentTerm
		return
	} else if args.Term > rf.currentTerm {	// 说明你的term更新了
		rf.safeUpdateStatTo(Follower, -1, args.Term)
	}
	reply.Term = rf.currentTerm
	FirstIndex := rf.log[0].Index
	// 快照所带来的还不够
	if args.LastIncludedIndex < FirstIndex {
		reply.PrevIndex = FirstIndex
		fmt.Println("me = %v LastIncludedIndex = %v, FirstIndex = %v\n", rf.me, args.LastIncludedIndex, FirstIndex)
		return
	}
	rf.log = args.Entries
	rf.snapshottedIndex = args.LastIncludedIndex
//	if rf.lastApplied < args.LastIncludedIndex {
	rf.lastApplied = args.LastIncludedIndex
//	}
//	if rf.commitIndex < args.LeaderCommit {
	rf.commitIndex = args.LeaderCommit
//	}
	rf.persister.SaveStateAndSnapshot(rf.getPersistData(), args.Data)
	msg := ApplyMsg{}
	msg.Data = args.Data
	msg.CommandValid = false
	rf.applyCh <- msg
	reply.Success = true
	fmt.Printf("%v install success replay = %v, FirstIndex = %v\n",rf.me, rf.lastApplied, rf.log[0].Index)
	go func() {
		rf.appendCh <- struct{}{}
	}()
}
func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply)  bool{
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

// 发送数据/作为心跳时 发送参数
type AppendEntriesArgs struct {
	Term int			// term
	LeaderId int		// 发送者的id
	PrevLogIndex int	// 上一个index是多少  只有这两个参数都一样时，才可以接受本数据
	PrevLogTerm int		// 上一个term时多少 只有这两个参数都一样时，才可以接受本数据
	Entries []Entry		// 数据 即log
	LeaderCommit int	// 可以commit的编号
}
// 返回参数
type AppendEntriesReply struct {
	Term int			// 当期iterm
	Success bool		// 是否PrevLogIndex与PrevLogTerm一致，不一致代表需要中间部分数据
	LastLogIndex int	// 最后一个log的index, 即需要的最小index
}
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	reply.Success = false
	if rf.stat == Dead {
//		return
	}
	fmt.Printf("me = %v Leader = %v, prevlogterm=%v, prevlogindex=%v,EntryLen=%v",
				rf.me, args.LeaderId,args.PrevLogTerm,args.PrevLogIndex,len(args.Entries))

	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		reply.LastLogIndex = rf.log[len(rf.log) - 1].Index
		return
	} else if args.Term > rf.currentTerm {
		rf.safeUpdateStatTo(Follower, -1, args.Term)
	}

	// 由于log不断增加，因此之前部分log可能会删除，log[0]的index可能不一样，PrevIndex则是当前raft下的偏移
	FirstIndex := rf.log[0].Index
	PrevIndex := args.PrevLogIndex - FirstIndex
	if PrevIndex < 0 {
		reply.LastLogIndex = rf.log[len(rf.log)-1].Index
		return
	}
	if args.PrevLogIndex != -1 && args.PrevLogTerm != -1 {
		if len(rf.log) - 1 < PrevIndex || rf.log[PrevIndex].Term != args.PrevLogTerm {	// 数据太新或者term已经分叉
			reply.Success = false
			if len(rf.log) - 1 >= PrevIndex {	// 数据分叉,之前Leader存储了垃圾log
				tmp := PrevIndex
				for tmp > 0 && rf.log[tmp].Term == rf.log[PrevIndex].Term {
					tmp--
				}
				rf.log = rf.log[:tmp+1]
				rf.persist()
			}					// else 说明数据太新，中间就数据还没更新过来呢
		//} else if rf.stat == Follower && rf.commitIndex <= args.LeaderCommit{
		} else if rf.stat == Follower {
			rf.mu.Lock()
			reply.Success = true
			j := 0
			// 将已经存在且正确的数据保留，多余的舍弃
			for i := PrevIndex + 1; i < len(rf.log) && j <len(args.Entries); i++{
				if rf.log[i].Term != args.Entries[j].Term {
					rf.log = rf.log[:i]
					break
				}
				j++
			}

			// j之后则是本节点既不存在，又需要保存的log
			rf.log = append(rf.log, args.Entries[j:]...)
			rf.persist()
			fmt.Printf("%v add len = %v\n", rf.me, len(args.Entries) - j)

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
		} else {	// 可能是Candidate/Dead等,或者拥有相同数量上一term的数据，但是commit还没更新
		}
	}

	reply.Term = rf.currentTerm
	reply.LastLogIndex = rf.log[len(rf.log) - 1].Index

	fmt.Printf("reply me = %v lastLogIndex=%v,success?= %v\n",rf.me,reply.LastLogIndex, reply.Success)

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
	reply.VoteGranted = false
	if rf.stat == Dead {
	//	return
	}
	fmt.Printf("me = %v arg.term = %v my.term = %v\n",rf.me ,args.Term, rf.currentTerm)

	if args.Term>rf.currentTerm{
		rf.safeUpdateStatTo(Follower, -1, args.Term)
	}
	if args.Term >= rf.currentTerm && (rf.voteFor == -1 || rf.voteFor == args.CandidateId){
		lastLogIndex := rf.log[len(rf.log) - 1].Index
		lastLogTerm := rf.log[len(rf.log) - 1].Term
		fmt.Printf("me = %v, myIndex = %v, argsIndex = %v, myTerm = %v argsTerm = %v\n",rf.me, lastLogIndex, args.LastLogIndex, lastLogTerm, args.LastLogTerm)
		// arg日志更多，则投票
		if (lastLogTerm < args.LastLogTerm) || (lastLogTerm == args.LastLogTerm && lastLogIndex <= args.LastLogIndex) {
			reply.VoteGranted = true
			rf.mu.Lock()
			rf.voteFor = args.CandidateId
			rf.mu.Unlock()
			rf.persist()
		}
	}
	reply.Term = rf.currentTerm
	if args.Term >= rf.currentTerm {
		go func() {
			rf.voteCh <- struct{}{}
		}()
	}
}

func (rf *Raft) safeUpdateStatTo(stat int, voteFor int, term int) {
	rf.mu.Lock()
	if rf.stat == Dead {
		rf.mu.Unlock()
		return
	}
	if stat == Follower {
		rf.stat = Follower
		if rf.voteFor != -2 {
			rf.voteFor = voteFor
			rf.currentTerm = term
		}
		fmt.Printf("%v update to Follower\n", rf.me)
	} else if stat == Candidate {
		rf.stat = Candidate
		fmt.Printf("%v update to Candidate\n", rf.me)
	} else if stat == Leader{
		rf.stat = Leader
		rf.nextIndex = make([]int, len(rf.peers))
		rf.matchIndex = make([]int, len(rf.peers))
		for i, _ :=range rf.peers{
			if i != rf.me {
				rf.nextIndex[i] = rf.log[len(rf.log)-1].Index + 1
				rf.matchIndex[i] = 0
			}
		}
		fmt.Printf("%v update to Leader\n",rf.me)
	}
	rf.mu.Unlock()
	rf.persist()
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// 返回下一个可以作为提交的index,并且把该cmd包装到log中，等待commit
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	rf.mu.Lock()
	term = rf.currentTerm
	isLeader = (rf.stat == Leader)
	rf.mu.Unlock()
	if isLeader{
		rf.mu.Lock()
		index = rf.log[len(rf.log)-1].Index+1
		entry := Entry{term, index, command}
		rf.log = append(rf.log, entry)
		rf.mu.Unlock()
		rf.persist()
	}
	fmt.Printf("me = %v, term = %v isLeader = %v\n",rf.me, term, isLeader)
	return index, term, isLeader
}

func (rf *Raft) Kill() {
	fmt.Printf("me = %v wait lock to be killed\n")
	rf.mu.Lock()
	fmt.Printf("me = %v has get lock to be killed\n")
	rf.stat = Dead
	rf.mu.Unlock()
	fmt.Printf("raft %v is dead\n",rf.me);
}
// 成为候选人之后，试图进行选举
func (rf *Raft) startElection() {
	if rf.stat == Dead{
		return
	}
	fmt.Printf("%v begin start election \n", rf.me)
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
		args.LastLogIndex = rf.log[len(rf.log) - 1].Index
		args.LastLogTerm = rf.log[len(rf.log) - 1].Term
	}

	for i, _:=range rf.peers {
		if i != rf.me {
			go func(server int) {
				reply := RequestVoteReply{}
				ok := rf.sendRequestVote(server, &args, &reply)
				if rf.stat == Candidate && ok && reply.Term == rf.currentTerm{
					if reply.VoteGranted {	// else说明投票给别人
						fmt.Printf("%v vote for me %v\n",server,rf.me)
						rf.mu.Lock()
						rf.voteCount += 1
						rf.mu.Unlock()
					}
				} else if rf.stat == Candidate && ok && reply.Term > rf.currentTerm {
					rf.safeUpdateStatTo(Follower, -1, reply.Term)
				}
			}(i)
		}
	}
}
// 为每个节点追加log
func (rf *Raft) broadcastAppendEntriesRPC() {
	for i, _ :=range rf.peers {
		if i != rf.me {
			go func(server int) {
				FirstIndex := rf.log[0].Index
				// <说明日志已经删除，只能获取快照
				fmt.Printf("Leader = %v peer %v matchIndex = %v nextIndex = %v, FirstIndex = %v\n", rf.me, server, rf.matchIndex[server], rf.nextIndex[server], FirstIndex)
				if rf.nextIndex[server] > FirstIndex {
					args := AppendEntriesArgs{}
					reply := AppendEntriesReply{}
					args.Term = rf.currentTerm
					args.LeaderId = rf.me
					args.LeaderCommit = rf.commitIndex
					args.PrevLogIndex = -1
					args.PrevLogTerm = -1

					index := max(min(rf.matchIndex[server], rf.log[len(rf.log)-1].Index) - FirstIndex, 0)
					if len(rf.log) > 0 && len(rf.log) > index {
						args.PrevLogIndex = rf.log[index].Index
						args.PrevLogTerm = rf.log[index].Term
						if index < rf.log[len(rf.log)-1].Index {	// 发送index之后的
							args.Entries = rf.log[index+1:]
						} else {
							args.Entries = make([]Entry, 0)
						}
					}
					ok := rf.sendAppendEntries(server, &args, &reply)
					ok = ok && reply.Term == rf.currentTerm

					// 有更新term, 自动降级为Follower
					if reply.Term > rf.currentTerm {
						rf.safeUpdateStatTo(Follower, -1, reply.Term)
					}
					if ok && rf.stat == Leader && reply.Success == true{
						if len(rf.log) > 0 && len(args.Entries) > 0 {
							rf.nextIndex[server] = args.Entries[len(args.Entries) - 1].Index + 1
							rf.matchIndex[server] = rf.nextIndex[server] - 1
						}
						fmt.Printf("Leader = %v, 给 %v 数据追加成功,matchIndex = %v, my commitIndex = %v\n",rf.me, server, rf.matchIndex[server], rf.commitIndex)

						// 这个节点掌握的数据可能还没commit
						if rf.commitIndex < rf.matchIndex[server] {
							rf.updateCommitIndex()
							if rf.commitIndex <= reply.LastLogIndex {
								rf.commitCh <-struct{}{}
							}
						}
					}else if rf.stat == Leader{
						// TODO ok??
						rf.nextIndex[server] = max(rf.matchIndex[server], reply.LastLogIndex) + 1
					}
				} else {
					fmt.Printf("leader = %v Need Send Snapshot to %v\n", rf.me, server)
					if rf.log[0].Index != rf.snapshottedIndex {
						fmt.Printf("TODO me = %v log0 != snapshottedindex\n", rf.me)
					}
					args := InstallSnapshotArgs{rf.currentTerm, rf.me, rf.commitIndex, rf.log[0].Index, rf.log[0].Term, rf.persister.ReadSnapshot(), rf.log}
					reply := InstallSnapshotReply{}
					ok := rf.sendInstallSnapshot(server, &args, &reply)
					fmt.Printf("already send ok = %v reply = %v\n", ok, reply)
					if reply.Term > rf.currentTerm {
						rf.safeUpdateStatTo(Follower, -1, reply.Term)
					}
					if ok && rf.stat == Leader {
						if reply.Success {
							rf.nextIndex[server] = args.Entries[len(args.Entries)-1].Index+1
							rf.matchIndex[server] = rf.nextIndex[server] - 1

							if rf.commitIndex < rf.matchIndex[server] {
								rf.updateCommitIndex()
								rf.commitCh <- struct{}{}
							}
						} else {
							rf.nextIndex[server] = reply.PrevIndex + 1
						}
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
			case <-rf.commitCh:
			for {
				if rf.commitIndex > rf.lastApplied {
					FirstIndex := rf.log[0].Index
					// TODO 如果小于，是否说明lastApplied数据有问题,如读取了其他的快照导致apply已经落后了
					if rf.lastApplied + 1 >= FirstIndex {
						msg := ApplyMsg{}

						rf.mu.Lock()
						rf.lastApplied += 1
						index := min(rf.lastApplied - rf.log[0].Index, len(rf.log) - 1)
						if(rf.log[len(rf.log) - 1].Index < rf.lastApplied){
							fmt.Printf("TODO lastLog < lastApplied ???\n")
						}
						msg.Command = rf.log[index].Command
						msg.CommandIndex = rf.lastApplied
						rf.mu.Unlock()

						// 真正在本节点提交数据,编号为rf.lastApplied
						fmt.Printf("%v Commit! commitIndex = %v apply = %v Command = %v\n",rf.me ,rf.commitIndex, rf.lastApplied, msg.Command)
						msg.CommandValid = true
						rf.applyCh <- msg
					} else {
						fmt.Printf("LMH Do %v Commit! apply = %v < FirstIndex = %v\n",rf.me ,rf.lastApplied, FirstIndex)
						log.Fatal("commit error:")
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
				// TODO rf.log[i - FirstIndex].Term == rf.currentTerm 有没有必要
				if rf.matchIndex[j] >= i && rf.log[i - FirstIndex].Term == rf.currentTerm{
					cnt++
				}
			}
		}
		if cnt > len(rf.peers)/2 {
			N = i
		} else {
			// 不能break,虽然之前的不是当前term产生的数据，但如果其他peers都追上来，允许N在较大时候直接commit
		}
	}
	if N > rf.commitIndex && rf.stat == Leader {
		rf.mu.Lock()
		rf.commitIndex = N
		rf.mu.Unlock()
		rf.persist()
		fmt.Printf("Leader = %v updateCommitIndex = %v\n", rf.me, N)
	}
}
// 无限循环在Follower/Candidate/Leader
func (rf *Raft) mainLoop() {
	for {
		// fmt.Printf("my = %v term = %v lastindex = %v lastterm = %v\n", rf.me, rf.currentTerm, rf.log[len(rf.log) -1].Term, rf.log[len(rf.log) -1].Index)
		switch rf.stat{
		case Follower:
			// 追加数据,或者请求投票会让时间重置,超时则成为候选人
			select {
			case <- rf.appendCh:
				rf.timer.Reset(getRandomTimeOut())
			case <-rf.voteCh:
				rf.timer.Reset(getRandomTimeOut())
			case <-rf.timer.C:
				rf.safeUpdateStatTo(Candidate, -1, -1)
				rf.startElection()
			}
		case Candidate:
			// 收到数据说明有Leader,否则请求投票,票数过半则成为Leader
			select {
			case <- rf.appendCh:
				rf.timer.Reset(getRandomTimeOut())
				rf.safeUpdateStatTo(Follower, -2, -2)
			case <-rf.timer.C:
				rf.startElection()
			default:
				if rf.voteCount > len(rf.peers)/2 {
					rf.safeUpdateStatTo(Leader, -2, -2)
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

// 初始化生成Raft节点, peers为其他raft节点, me就是自己编号
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
		rf.snapshottedIndex = 0
		rf.log = make([]Entry, 0)
		rf.log = append(rf.log, Entry{0, 0, 0})
		rf.voteCh = make(chan struct{})
		rf.appendCh = make(chan struct{})
		rf.commitCh = make(chan struct{})
		rf.applyCh = applyCh

		rf.timer = time.NewTimer(getRandomTimeOut())
		go rf.mainLoop()
		go rf.doCommitLoop()
		rf.mu.Lock()
		rf.readPersist(persister.ReadRaftState())
		rf.mu.Unlock()

		return rf
}
