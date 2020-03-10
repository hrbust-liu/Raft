package raftkv

import "labrpc"
import "testing"
import "os"

import crand "crypto/rand"
import "math/big"
import "math/rand"
import "encoding/base64"
import "sync"
import "runtime"
import "raft"
import "fmt"
import "time"
import "sync/atomic"

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

func random_handles(kvh []*labrpc.ClientEnd) []*labrpc.ClientEnd {
	sa := make([]*labrpc.ClientEnd, len(kvh))
	copy(sa, kvh)
	for i := range sa {
		j := rand.Intn(i + 1)
		sa[i], sa[j] = sa[j], sa[i]
	}
	return sa
}
// 客户端进行Get/Put/Append操作时会和Clerk进行交互，Clerk与KVServer进行通信(寻找Leader等)完成后返回给客户端
type config struct {
	mu			sync.Mutex
	t			*testing.T
	net			*labrpc.Network		// network
	n			int
	kvservers	[]*KVServer			// kvserver
	saved		[]*raft.Persister
	endnames	[][]string			// endname
	clerks		map[*Clerk][]string	// clerk->endname
	nextClientId	int				// 再次创建Client的id
	maxraftState	int
	start			time.Time		// 创建环境的时间

	t0		time.Time			// 启动时间
	rpcs0	int					// 启动时rpc次数
	ops		int32				// ops次数
}
// 判断是否超时
func (cfg *config) checkTimeout() {
	if !cfg.t.Failed() && time.Since(cfg.start) > 120*time.Second {
		cfg.t.Fatal("test longer than 120 seconds")
	}
}
// kill所有kvserver
func (cfg *config) cleanup() {
	cfg.mu.Lock()
	defer cfg.mu.Unlock()
	for i := 0; i <len(cfg.kvservers); i++ {
		if cfg.kvservers[i] != nil {
			cfg.kvservers[i].Kill()
		}
	}
	cfg.net.Cleanup()
	cfg.checkTimeout()
}
// 求出最多的占用字节数
func (cfg *config) LogSize() int {
	logsize := 0
	for i := 0; i< cfg.n; i++ {
		n := cfg.saved[i].RaftStateSize()
		if n > logsize {
			logsize = n
		}
	}
	return logsize
}
//	快照大小
func (cfg *config) SnapshotSize() int {
	snapshotsize := 0
	for i := 0; i < cfg.n; i++ {
		n := cfg.saved[i].SnapshotSize()
		if n > snapshotsize {
			snapshotsize = n
		}
	}
	return snapshotsize
}
// 无锁的连接
func (cfg *config) connectUnlocked(i int, to []int) {
	for j := 0; j < len(to); j++ {
		endname := cfg.endnames[i][to[j]]
		cfg.net.Enable(endname, true)
	}

	for j := 0; j < len(to); j++ {
		endname := cfg.endnames[to[j]][i]
		cfg.net.Enable(endname, true)
	}
}
// 加锁的连接
func (cfg *config) connect(i int, to []int) {
	cfg.mu.Lock()
	defer cfg.mu.Unlock()
	cfg.connectUnlocked(i, to)
}
// 无锁的断开连接
func (cfg *config) disconnectUnlocked(i int, from []int) {
	for j := 0; j <len(from); j++ {
		if cfg.endnames[i] != nil {
			endname := cfg.endnames[i][from[j]]
			cfg.net.Enable(endname, false)
		}
	}

	for j := 0; j < len(from); j++ {
		if cfg.endnames[j] != nil {
			endname := cfg.endnames[from[j]][i]
			cfg.net.Enable(endname, false)
		}
	}
}
// 加锁的断开连接
func (cfg *config) disconnect(i int, from []int) {
	cfg.mu.Lock()
	defer cfg.mu.Unlock()
	cfg.disconnectUnlocked(i, from)
}
func (cfg *config) All() []int {
	all := make([]int, cfg.n)
	for i := 0; i < cfg.n; i++ {
		all[i] = i
	}
	return all
}

func (cfg *config) ConnectAll() {
	cfg.mu.Lock()
	defer cfg.mu.Unlock()
	for i := 0; i < cfg.n; i++ {
		cfg.connectUnlocked(i, cfg.All())
	}
}
// 进行分区,p1内部互相通信,p2内部互相通信,p1与p2不通信
func (cfg *config) partition(p1 []int, p2 []int) {
	cfg.mu.Lock()
	defer cfg.mu.Unlock()
	for i := 0; i < len(p1); i++ {
		cfg.disconnectUnlocked(p1[i], p2)
		cfg.connectUnlocked(p1[i], p1)
	}
	for i := 0; i <len(p2); i++ {
		cfg.disconnectUnlocked(p2[i], p1)
		cfg.connectUnlocked(p2[i], p2)
	}
}
// 制作客户端,创建endname,绑定到network
func (cfg *config) makeClient(to []int) *Clerk {
	cfg.mu.Lock()
	defer cfg.mu.Unlock()

	ends := make([]*labrpc.ClientEnd, cfg.n)
	endnames := make([]string, cfg.n)
	for j := 0; j< cfg.n;j++ {
		endnames[j] = randstring(20)		// 将endname创建，并让network进行记录,理解为与j节点通信的端口
		ends[j] = cfg.net.MakeEnd(endnames[j])
		cfg.net.Connect(endnames[j], j)
	}

	ck := MakeClerk(random_handles(ends))
	cfg.clerks[ck] = endnames
	cfg.nextClientId++
	cfg.ConnectClientUnlocked(ck, to)
	return ck
}
// 删除客户端
func (cfg *config) deleteClient(ck *Clerk) {
	cfg.mu.Lock()
	defer cfg.mu.Unlock()

	v := cfg.clerks[ck]
	for i := 0; i < len(v); i++ {
		os.Remove(v[i])
	}
	delete(cfg.clerks, ck)
}
// 将客户端连接哦,通过设置endname有效
func (cfg *config) ConnectClientUnlocked(ck *Clerk, to []int) {
	endnames := cfg.clerks[ck]
	for j := 0; j < len(to); j++ {
		s := endnames[to[j]]
		cfg.net.Enable(s, true)
	}
}

func (cfg *config) ConnectClient(ck *Clerk, to []int) {
	cfg.mu.Lock()
	defer cfg.mu.Unlock()
	cfg.ConnectClientUnlocked(ck, to)
}
// 将客户端断开连接,通过设置endname无效
func (cfg *config) DisconnectClientUnlocked(ck *Clerk, from []int) {
	endnames := cfg.clerks[ck]
	for j := 0; j < len(from); j++ {
		s := endnames[from[j]]
		cfg.net.Enable(s, false)
	}
}

func (cfg *config) DisconnectClient(ck *Clerk, from []int) {
	cfg.mu.Lock()
	defer cfg.mu.Unlock()
	cfg.DisconnectClientUnlocked(ck, from)
}
// 停止server,断开连接，删除server,保存数据,最后kill
func (cfg *config) ShutdownServer(i int) {
	cfg.mu.Lock()
	defer cfg.mu.Unlock()

	cfg.disconnectUnlocked(i, cfg.All())

	cfg.net.DeleteServer(i)

	if cfg.saved[i] != nil {
		cfg.saved[i] = cfg.saved[i].Copy()
	}

	kv := cfg.kvservers[i]
	if kv != nil {
		cfg.mu.Unlock()
		kv.Kill()
		cfg.mu.Lock()
		cfg.kvservers[i] = nil
	}
}
// 创建server,创建与其他server的借口(endname)
func (cfg *config) StartServer(i int) {
	cfg.mu.Lock()

	cfg.endnames[i] = make([]string, cfg.n)
	for j := 0; j < cfg.n; j++ {
		cfg.endnames[i][j] = randstring(20)
	}

	ends := make([]*labrpc.ClientEnd, cfg.n)		// 创建端口,并与不同server连接
	for j := 0; j < cfg.n; j++ {
		end[j] = cfg.net.MakeEnd(cfg.endnames[i][j])
		cfg.net.Connect(cfg.endnames[i][j], j)
	}

	if cfg.saved[i] != nil {
		cfg.saved[i] = cfg.saved[i].Copy()
	} else {
		cfg.saved[i] = raft.MakePersister()
	}
	cfg.mu.Unlock()

	cfg.kvservers[i] = StartKVServer(ends, i, cfg.saved[i], cfg.maxraftstate)

	kvsvc := labrpc.MakeService(cfg.kvservers[i])		// 解析kvserver类型,用于Call
	rfsvc := labrpc.MakeService(cfg.kvservers[i].rf)	// 解析raft 类型
	srv := labrpc.MakeServer()							// 创建服务
	srv.AddService(kvsvc)								// kvserver类型添加到服务
	srv.AddService(rfsvc)								// raft类型添加到服务
	cfg.net.AddServer(i, srv)
}
// 找到Leader返回(true, leader编号)，找不到返回(false, 0)
func (cfg *config) Leader() (bool, int) {
	cfg.mu.Lock()
	defer cfg.mu.Unlock()

	for i := 0; i <cfg.n;i++ {
		_, is_leader := cfg.kvservers[i].rf.GetState()
		if is_leader {
			return true, i
		}
	}
	return false, 0
}
// 分成两组返回,leader放到第二组
func (cfg *config) make_partition() ([]int, []int) {
	_, l := cfg.Leader()
	p1 := make([]int, cfg.n/2+1)
	p2 := make([]int, cfg.n/2)
	j := 0
	for i := 0; i < cfg.n; i++ {
		if i != l {
			if j < len(p1) {
				p1[j] = i
			} else {
				p2[j-len(p1)] = i
			}
			j++
		}
	}
	p2[len(p2)-1] = l
	return p1, p2
}
var ncpu_once sync.Once
// 制作环境
func make_config(t *testing.T, n int, unrelibale bool, maxraftstate int) *config {
	ncpu_once.Do(func() {
		if runtime.NumCPU() <2 {
			fmt.Printf("warning: only one CPU, which may conceal locking bugs\n")
		}
		rand.Seed(makeSeed())
	})
	runtime.GOMAXPROCS(4)
	cfg := &config{}
	cfg.t = t
	cfg.net = labrpc.MakeNetwork()
	cfg.n = n
	cfg.kvservers = make([]*KVServer, cfg.n)
	cfg.saved = make([]*raft.Persister, cfg.n)
	cfg.endnames = make([][]]string, cfg.n)
	cfg.clerks = make(map[*Clerk][]string)
	cfg.nextClientId = cfg.n + 1000
	cfg.maxraftstate = maxraftstate
	cfg.start = time.Now()

	for i := 0; i < cfg.n; i++ {
		cfg.StartServer(i)
	}

	cfg.ConnectAll()
	cfg.net.Reliable(!unreliable)
	return cfg
}

// rpc总次数
func (cfg *config) rpcTotal() int {
	return cfg.net.GetTotalCount()
}
// 启动,并记录信息
func (cfg *config) begin(description string) {
	fmt.Printf("%s ...\n", description)
	cfg.t0 = time.Now()				// 记录启动时间
	cfg.rpcs0 = cfg.rpcTotal()		// 已经使用的rpc次数
	atomic.StoreInt32(&cfg.ops, 0)	// ops设置为0
}
// 原子加一
func (cfg *config) op() {
	atomic.AddInt32(&cfg.ops, 1)
}
// 结束时判断是否出错
func (cfg *config) end() {
	cfg.checkTimeout()
	if cfg.t.Failed() == false {
		t := time.Since(cfg.t0).Seconds()
		nppers := cfg.n
		nrprc := cfg.rpcTotal() - cfg.rpcs0
		ops := atomic.LoadInt32(&cfg.ops)

		fmt.Printf("  ...Passed..")
		fmt.Printf("  %4.1f %d %5d %4d\n", t, npeers, nrpc, ops)
	}

