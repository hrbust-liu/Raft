package raft

import "labrpc"

import "log"
import "sync"
import "testing"
import "runtime"
import "math/rand"
import crand "crypto/rand"
import "math/big"
import "encoding/base64"
import "time"
import "fmt"

func randstring(n int) string {
	b := make([]byte, 2*n)
	crand.Read(b)
	s := base64.URLEncoding.EncodeToString(b)
	return s[0:n]
}

func makeSeed() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := crand.Int(crand.Reader, max)
	x := bigx.Int64()
	return x
}

type config struct {
	mu			sync.Mutex
	t			*testing.T
	net			*labrpc.Network
	n			int				// 节点个数
	rafts		[]*Raft			// 每个节点Raft
	applyErr	[]string		// 
	connected	[]bool			// 是否还在连接
	saved		[]*Persister	// 每个节点的持久化
	endnames	[][]string		// 接口文件名
	logs		[]map[int]int
	start		time.Time		// 开始时间

	t0			time.Time
	rpcs0		int
	cmds0		int
	maxIndex	int
	maxIndex0	int
}

var ncpu_once sync.Once
// 初始化环境配置参数,MakeNetWork将初始化环境
func make_config(t *testing.T, n int, unreliable bool) *config {
	ncpu_once.Do(func() {
		if runtime.NumCPU() < 2 {
			fmt.Printf("warning: only one CPU")
		}
		rand.Seed(makeSeed())
	})
	runtime.GOMAXPROCS(4)
	cfg := &config{}
	cfg.t = t
	cfg.net = labrpc.MakeNetwork()
	cfg.n = n
	cfg.applyErr = make([]string, cfg.n)
	cfg.rafts = make([]*Raft, cfg.n)
	cfg.connected = make([]bool, cfg.n)
	cfg.saved = make([]*Persister, cfg.n)
	cfg.endnames = make([][]string, cfg.n)
	cfg.logs = make([]map[int]int, cfg.n)
	cfg.start = time.Now()

	cfg.setunreliable(unreliable)

	cfg.net.LongDelays(true)

	for i := 0; i < cfg.n; i++ {
		cfg.logs[i] = map[int]int{}
		cfg.start1(i)
	}

	for i := 0; i < cfg.n; i++ {
		cfg.connect(i)
	}

	return cfg
}
// 数据复制一份,状态全部关闭
func (cfg *config) crash1(i int) {
	cfg.disconnect(i)
	cfg.net.DeleteServer(i)

	cfg.mu.Lock()
	defer cfg.mu.Unlock()

	if cfg.saved[i] != nil {
		cfg.saved[i] = cfg.saved[i].Copy()
	}

	rf := cfg.rafts[i]
	if rf != nil {
		cfg.mu.Unlock()
		rf.Kill()
		cfg.mu.Lock()
		cfg.rafts[i] = nil
	}

	if cfg.saved[i] != nil{
		raftlog := cfg.saved[i].ReadRaftState()
		cfg.saved[i] = &Persister{}
		cfg.saved[i].SaveRaftState(raftlog)
	}
}
// i节点开始，创建endname并且将收到数据进行检测是否与其他有不同
func (cfg *config) start1(i int) {
	cfg.crash1(i)

	cfg.endnames[i] = make([]string, cfg.n)
	for j := 0; j < cfg.n; j++ {
		cfg.endnames[i][j] = randstring(20)
	}

	ends := make([]*labrpc.ClientEnd, cfg.n)
	for j:=0;j<cfg.n;j++ {
		ends[j] = cfg.net.MakeEnd(cfg.endnames[i][j])
		cfg.net.Connect(cfg.endnames[i][j], j)
	}

	cfg.mu.Lock()

	if cfg.saved[i] != nil {
		cfg.saved[i] = cfg.saved[i].Copy()
	} else {
		cfg.saved[i] = MakePersister()
	}

	cfg.mu.Unlock()

	applyCh := make(chan ApplyMsg)
	go func() {
		for m := range applyCh {
			err_msg := ""
			if m.CommandValid == false {
			} else if v, ok := (m.Command).(int); ok {
				// 检测数据是否正确
				cfg.mu.Lock()
				for j := 0; j< len(cfg.logs); j++ {
					if old, oldok := cfg.logs[j][m.CommandIndex]; oldok && old != v {
						err_msg = fmt.Sprintf("commit data wrong!! index=%v server=%v %v != server=%v %v",
							m.CommandIndex, i, m.Command, j, old)
					}
				}
				_, prevok := cfg.logs[i][m.CommandIndex - 1]
				cfg.logs[i][m.CommandIndex] = v
				if m.CommandIndex > cfg.maxIndex {
					cfg.maxIndex = m.CommandIndex
				}
				cfg.mu.Unlock()

				if m.CommandIndex > 1 && prevok == false {
					err_msg = fmt.Sprintf("server %v apply out of order %v", i, m.CommandIndex)
				}
			} else {
				err_msg = fmt.Sprintf("committed command %v is not an int", m.Command)
			}

			if err_msg != "" {
				log.Fatalf("apply error: %v\n", err_msg)
				cfg.applyErr[i] = err_msg
			}
		}
	}()
	// 该节点开启,commit数据传入applyCh
	rf := Make(ends, i, cfg.saved[i], applyCh)

	cfg.mu.Lock()
	cfg.rafts[i] = rf
	cfg.mu.Unlock()

	svc := labrpc.MakeService(rf)	// 解析一个类型，如raft做成反射服务
	srv := labrpc.MakeServer()
	srv.AddService(svc)				// 添加一个类型,每个节点可以有很多类型,每个类型有很多方法
	cfg.net.AddServer(i, srv)		// 将其放入该节点下
}
func (cfg *config) checkTimeout() {
	if !cfg.t.Failed() && time.Since(cfg.start) > 120*time.Second {
		cfg.t.Fatal("test took longer than 120 s")
	}
}
func (cfg *config) cleanup() {
	for i := 0; i <len(cfg.rafts); i++ {
		if cfg.rafts[i] != nil {
			cfg.rafts[i].Kill()
		}
	}
	cfg.net.Cleanup()
	cfg.checkTimeout()
}
// 建立i与其他节点的连接
func (cfg *config) connect(i int) {
	fmt.Printf("%v connect\n",i)
	cfg.connected[i] = true

	for j := 0; j<cfg.n;j++{
		if cfg.connected[j] {
			endname := cfg.endnames[i][j]
			cfg.net.Enable(endname, true)
		}
	}

	for j := 0; j < cfg.n; j++ {
		if cfg.connected[j] {
			endname := cfg.endnames[j][i]
			cfg.net.Enable(endname, true)
		}
	}
}
// 断开i与其他节点的连接
func (cfg *config) disconnect(i int) {
	fmt.Printf("%v disconnect\n",i)
	cfg.connected[i] = false

	for j := 0; j < cfg.n; j++ {
		if cfg.endnames[i] != nil{
			endname := cfg.endnames[i][j]
			cfg.net.Enable(endname, false)
		}
	}

	for j := 0; j < cfg.n; j++ {
		if cfg.endnames[j] != nil {
			endname := cfg.endnames[j][i]
			cfg.net.Enable(endname, false)
		}
	}
}

func (cfg *config) rpcCount(server int) int {
	return cfg.net.GetCount(server)
}

func (cfg *config) rpcTotal() int {
	return cfg.net.GetTotalCount()
}

func (cfg *config) setunreliable(unrel bool) {
	cfg.net.Reliable(!unrel)
}

func (cfg *config) setlongreordering(longrel bool) {
	cfg.net.LongReordering(longrel)
}
// 检查任何时刻是否仅有1个leader，由于leader选举需要时间，因此循环10次
func (cfg *config) checkOneLeader() int {
	for iters :=0; iters < 10; iters++ {
		ms := 450 + (rand.Int63() % 100)
		time.Sleep(time.Duration(ms) * time.Millisecond)

		leaders := make(map[int][]int)
		for i :=0;i<cfg.n;i++{
			if cfg.connected[i] {
				if cfg.connected[i] {
					if term, leader := cfg.rafts[i].GetState(); leader {
						leaders[term] =append(leaders[term], i)
					}
				}
			}
		}

		lastTermWithLeader := -1
		for term, leaders := range leaders {
			if len(leaders) > 1 {
				cfg.t.Fatal("term %d has %d (>1) leaders", term, len(leaders))
			}
			if term > lastTermWithLeader {
				lastTermWithLeader = term
			}
		}

		if len(leaders) != 0 {
			return leaders[lastTermWithLeader][0]
		}
	}
	cfg.t.Fatal("expected one leader")
	return -1
}
// 是否term一致
func (cfg *config) checkTerms() int {
	term := -1
	for i:=0; i <cfg.n; i++ {
		if cfg.connected[i] {
			xterm, _ :=cfg.rafts[i].GetState()
			if term == -1 {
				term = xterm
			} else if term != xterm {
				cfg.t.Fatal("term has disagree")
			}
		}
	}
	return term
}
// 测试希望无leader
func (cfg *config) checkNoLeader() {
	for i := 0; i < cfg.n; i++ {
		if cfg.connected[i] {
			_, is_leader := cfg.rafts[i].GetState()
			if is_leader {
				cfg.t.Fatalf("expected no leader")
			}
		}
	}
}
// 确保提交的index数据都是一致的
func (cfg *config) nCommitted(index int) (int, interface{}) {
	count := 0
	cmd := -1
	for i := 0; i <len(cfg.rafts); i++ {
		if cfg.applyErr[i] != "" {
			cfg.t.Fatal(cfg.applyErr[i])
		}

		cfg.mu.Lock()
		cmd1, ok := cfg.logs[i][index]
		cfg.mu.Unlock()

		if ok {
			if count > 0 && cmd != cmd1 {
				cfg.t.Fatalf("committed values do not match: index %v, %v, %v\n",
						index, cmd, cmd1)
			}
			count += 1
			cmd = cmd1
		}
	}
	return count, cmd
}
// 等待300ms第index的数据,至少n个节点一致,获取命令
func (cfg *config) wait(index int, n int, startTerm int) interface{} {
	to := 10 * time.Millisecond
	for iters := 0; iters < 30; iters++ {
		nd, _ := cfg.nCommitted(index)
		if nd >= n {
			break
		}
		time.Sleep(to)
		if to < time.Second {
			to *= 2
		}
		if startTerm > -1 {
			for _, r := range cfg.rafts {
				if t, _ := r.GetState(); t > startTerm {
					return -1
				}
			}
		}
	}
	nd, cmd := cfg.nCommitted(index)
	if nd < n {
		cfg.t.Fatalf("only %d decided for index %d; wanted %d\n",
			nd, index, n)
	}
	return cmd
}
// 发送一个数据
func (cfg *config) one(cmd int, expectedServers int, retry bool) int {
	t0 := time.Now()
	starts := 0
	for time.Since(t0).Seconds() < 10 {
		index := -1
		for si :=0; si<cfg.n;si++{
			starts = (starts +1)%cfg.n
			var rf *Raft
			cfg.mu.Lock()
			if cfg.connected[starts] {
				rf = cfg.rafts[starts]
			}
			cfg.mu.Unlock()
			if rf != nil {
				index1, _, ok := rf.Start(cmd)	// 只有是leader才能发送成功
				if ok {
					index = index1
					break
				}
			}
		}
		if index !=-1{
			t1 := time.Now()
			for time.Since(t1).Seconds() < 2 {
				nd, cmd1 := cfg.nCommitted(index)
				if nd > 0 && nd >= expectedServers {
					if cmd2, ok := cmd1.(int); ok && cmd2 == cmd {
						return index
					}
				}
				time.Sleep(20 * time.Millisecond)
			}
			if retry == false {
				cfg.t.Fatalf("one(%v) failed to reach agreement", cmd)
			}
		} else {
			time.Sleep(50 * time.Millisecond)
		}
	}
	cfg.t.Fatalf("one(%v) failed to reach agreement", cmd)
	return -1
}
// 运行开始，用于测试记录好当前时间,rpc次数,cmd次数等
func (cfg *config) begin(description string) {
	fmt.Printf("%s ...\n", description)
	cfg.t0 = time.Now()
	cfg.rpcs0 = cfg.rpcTotal()
	cfg.cmds0 = 0
	cfg.maxIndex0 = cfg.maxIndex
}
// 运行结束，统计时间等
func (cfg *config) end() {
	cfg.checkTimeout()
	if cfg.t.Failed() == false {
		cfg.mu.Lock()
		t := time.Since(cfg.t0).Seconds()
		npeers := cfg.n
		nrpc := cfg.rpcTotal() - cfg.rpcs0
		ncmds := cfg.maxIndex - cfg.maxIndex0
		cfg.mu.Unlock()

		fmt.Printf(" ...Passed --")
		fmt.Printf(" %4.1f %d %4d %4d\n", t, npeers, nrpc, ncmds)
	}
}
