package raftkv

import "linearizability"

import "testing"
import "strconv"
import "time"
import "math/rand"
import "log"
import "strings"
import "sync"
import "sync/atomic"

const electionTimeout = 1 * time.Second

const linearizabilityCheckTimeout = 1 * time.Second

var Test3A = true

// Get/Put/Append/Check 操作
func Get(cfg *config, ck *Clerk, key string) string {
	v := ck.Get(key)
	cfg.op()
	return v
}

func Put(cfg *config, ck *Clerk, key string, value string) {
	ck.Put(key, value)
	cfg.op()
}

func Append(cfg *config, ck *Clerk, key string, value string) {
	ck.Append(key, value)
	cfg.op()
}

func check(cfg *config, t *testing.T, ck *Clerk, key string, value string) {
	v := Get(cfg, ck, key)
	if v != value {
		t.Fatalf("Get(%v): expected:\n%v\nreceived:\n%v", key, value, v)
	}
}

// 创建clerk执行fn函数，结束时向ca通道放入 ok(false/true)
func run_client(t *testing.T, cfg *config, me int, ca chan bool, fn func(me int, ck *Clerk, t *testing.T)) {
	ok := false
	defer func() { ca <- ok }()
	ck := cfg.makeClient(cfg.All())
	fn(me, ck, t)
	ok = true
	cfg.deleteClient(ck)
}

// 制作n个client判断他们是否成功
func spawn_clients_and_wait(t *testing.T, cfg *config, ncli int, fn func(me int, ck *Clerk, t *testing.T)) {
	ca := make([]chan bool, ncli)
	for cli := 0; cli < ncli; cli++ {
		ca[cli] = make(chan bool)
		go run_client(t, cfg, cli, ca[cli], fn)
	}
	for cli := 0; cli < ncli; cli++ {
		ok := <-ca[cli]
		if ok == false {
			t.Fatalf("failure")
		}
	}
}

// append之后的值
func NextValue(prev string, val string) string {
	return prev + val
}

// Append的格式应该是 "x clnt j y"连续的格式 其中clnt为client编号,j为第次个操作
func checkClntAppends(t *testing.T, clnt int, v string, count int) {
	lastoff := -1
	for j := 0; j < count; j++ {
		wanted := "x " + strconv.Itoa(clnt) + " " + strconv.Itoa(j) + " y"
		off := strings.Index(v, wanted)
		if off < 0 {
			t.Fatalf("%v 丢失数据 %v in Append result %v\n", clnt, wanted, v)
		}
		off1 := strings.LastIndex(v, wanted)
		if off1 != off {
			t.Fatalf("重复数据 %v in Append result\n", wanted)
		}
		if off <= lastoff {
			t.Fatalf("数据乱序 %v in Append result\n", wanted)
		}
		lastoff = off
	}
}

// 不断的将分区分为两组, 直到done被置1
func partitioner(t *testing.T, cfg *config, ch chan bool, done *int32) {
	defer func() { ch <- true }()
	for atomic.LoadInt32(done) == 0 {	// 不断进行分区,直到done为1
		a := make([]int, cfg.n)
		for i := 0; i < cfg.n; i++ {	// 随机分成两组
			a[i] = (rand.Int() % 2)
		}
		pa := make([][]int, 2)
		for i := 0; i < 2; i++ {
			pa[i] = make([]int, 0)
			for j := 0; j < cfg.n; j++ {
				if a[j] == i {
					pa[i] = append(pa[i], j)
				}
			}
		}
		cfg.partition(pa[0], pa[1])
		time.Sleep(electionTimeout + time.Duration(rand.Int63()%200)*time.Millisecond)
	}
}


// My test
func MyTest(t *testing.T, title string, nclients int, unreliable bool, crash bool, partitions bool, maxraftstate int) {

	const nservers = 5
	cfg := make_config(t, nservers, unreliable, maxraftstate)	// n个server启动，任意节点相互连接
	defer cfg.cleanup()

	cfg.begin(title)

	ck := cfg.makeClient(cfg.All())			// 制作clerk

	done_partitioner := int32(0)
	done_clients := int32(0)
	ch_partitioner := make(chan bool)		// 用来接受partation已经停止的信号
	clnts := make([]chan int, nclients)		// 制作client的通道
	for i := 0; i < nclients; i++ {
		clnts[i] = make(chan int)
	}
	for i := 0; i < 3; i++ {
		atomic.StoreInt32(&done_clients, 0)
		atomic.StoreInt32(&done_partitioner, 0)
		// 将会创建n个client并创建与之对应的clerk执行该函数
		go spawn_clients_and_wait(t, cfg, nclients, func(cli int, myck *Clerk, t *testing.T) {
			j := 0		// 记录了Append执行次数
			defer func() {
				clnts[cli] <- j
				DPrintf("%v quit\n", cli)
			}()
			last := ""
			key := strconv.Itoa(cli)
			Put(cfg, myck, key, last)	// my clerk put key-last
			for atomic.LoadInt32(&done_clients) == 0 {	// done_client 为0就一直执行,随机执行Append/Get操作
				if (rand.Int() % 1000) < 500 {
					nv := "x " + strconv.Itoa(cli) + " " + strconv.Itoa(j) + " y"
					 log.Printf("client %d new append %v\n", cli, nv)
					Append(cfg, myck, key, nv)
					last = NextValue(last, nv)
					j++
				} else {
					 log.Printf("client %d new get %v\n", cli, key)
					v := Get(cfg, myck, key)
					if v != last {
						log.Fatalf("get wrong value, key %v, wanted:\n%v\n, got\n%v\n", key, last, v)
					}
				}
			}
		})

		if partitions {	// 进行分区
			log.Printf("ready make partitions\n")
			time.Sleep(1 * time.Second)
			go partitioner(t, cfg, ch_partitioner, &done_partitioner)
		}
		time.Sleep(5 * time.Second)

		atomic.StoreInt32(&done_clients, 1)     // done_clients设置为1, 让client退出
		atomic.StoreInt32(&done_partitioner, 1) // 通知分区停止

		if partitions {
			<-ch_partitioner
			cfg.ConnectAll()
			time.Sleep(electionTimeout)
		}

		log.Printf("ready check clients\n")
		for i := 0; i < nclients; i++ {			// 检查value是否正确
			j := <-clnts[i]
			key := strconv.Itoa(i)
			 log.Printf("Check %v for client %d\n", j, i)
			v := Get(cfg, ck, key)
			checkClntAppends(t, i, v, j)
		}
	}

	cfg.end()
}
// LMH
func TestMy(t *testing.T) {
	return
	// Testing, title, nclient, unreliable, crash, partitions, maxraftstate
	Test3A = false
	MyTest(t, "Test1 client = 1", 1, false, false, false, -1)
	MyTest(t, "Test2 client = 5", 5, false, false, false, -1)
	MyTest(t, "Test3 client = 5, partitions = true", 5, true, false, true, -1)
	MyTest(t, "3A", 1, false, false, true, -1)

	MyTest(t, "3A", 5, false, false, true, -1)
	MyTest(t, "3A", 1, false, true, false, -1)
	MyTest(t, "3A", 5, false, true, false, -1)
	MyTest(t, "3A", 5, true, true, false, -1)
	MyTest(t, "3A", 5, false, true, true, -1)
	MyTest(t, "3A", 5, true, true, true, -1)
	//MyTestLinearizability(t, "3A", 15, 7, true, true, true, -1)
}

// check that all known appends are present in a value,
// and are in order for each concurrent client.
func checkConcurrentAppends(t *testing.T, v string, counts []int) {
	nclients := len(counts)
	for i := 0; i < nclients; i++ {
		lastoff := -1
		for j := 0; j < counts[i]; j++ {
			wanted := "x " + strconv.Itoa(i) + " " + strconv.Itoa(j) + " y"
			off := strings.Index(v, wanted)
			if off < 0 {
				t.Fatalf("%v missing element %v in Append result %v", i, wanted, v)
			}
			off1 := strings.LastIndex(v, wanted)
			if off1 != off {
				t.Fatalf("duplicate element %v in Append result", wanted)
			}
			if off <= lastoff {
				t.Fatalf("wrong order for element %v in Append result", wanted)
			}
			lastoff = off
		}
	}
}


// Basic test is as follows: one or more clients submitting Append/Get
// operations to set of servers for some period of time.  After the period is
// over, test checks that all appended values are present and in order for a
// particular key.  If unreliable is set, RPCs may fail.  If crash is set, the
// servers crash after the period is over and restart.  If partitions is set,
// the test repartitions the network concurrently with the clients and servers. If
// maxraftstate is a positive number, the size of the state for Raft (i.e., log
// size) shouldn't exceed 2*maxraftstate.
func GenericTest(t *testing.T, part string, nclients int, unreliable bool, crash bool, partitions bool, maxraftstate int) {

	title := "Test: "
	if unreliable {
		// the network drops RPC requests and replies.
		title = title + "unreliable net, "
	}
	if crash {
		// peers re-start, and thus persistence must work.
		title = title + "restarts, "
	}
	if partitions {
		// the network may partition
		title = title + "partitions, "
	}
	if maxraftstate != -1 {
		title = title + "snapshots, "
	}
	if nclients > 1 {
		title = title + "many clients"
	} else {
		title = title + "one client"
	}
	title = title + " (" + part + ")" // 3A or 3B

	const nservers = 5
	cfg := make_config(t, nservers, unreliable, maxraftstate)
	defer cfg.cleanup()

	cfg.begin(title)

	ck := cfg.makeClient(cfg.All())

	done_partitioner := int32(0)
	done_clients := int32(0)
	ch_partitioner := make(chan bool)
	clnts := make([]chan int, nclients)
	for i := 0; i < nclients; i++ {
		clnts[i] = make(chan int)
	}
	for i := 0; i < 3; i++ {
		 log.Printf("Iteration %v\n", i)
		atomic.StoreInt32(&done_clients, 0)
		atomic.StoreInt32(&done_partitioner, 0)
		go spawn_clients_and_wait(t, cfg, nclients, func(cli int, myck *Clerk, t *testing.T) {
			j := 0
			defer func() {
				DPrintf("%v wanna quit\n", cli)
				clnts[cli] <- j
				DPrintf("%v quit\n", cli)
			}()
			last := ""
			key := strconv.Itoa(cli)
			Put(cfg, myck, key, last)
			for atomic.LoadInt32(&done_clients) == 0 {
				if (rand.Int() % 1000) < 500 {
					nv := "x " + strconv.Itoa(cli) + " " + strconv.Itoa(j) + " y"
					 log.Printf("%d: client new append %v\n", cli, nv)
					Append(cfg, myck, key, nv)
					last = NextValue(last, nv)
					j++
				} else {
					 log.Printf("%d: client new get %v\n", cli, key)
					v := Get(cfg, myck, key)
					if v != last {
						log.Fatalf("%d client get wrong value, key %v, wanted:\n%v\n, got\n%v\n", cli, key, last, v)
					}
				}
			}
		})

		if partitions {
			log.Printf("make partition\n")
			// Allow the clients to perform some operations without interruption
			time.Sleep(1 * time.Second)
			go partitioner(t, cfg, ch_partitioner, &done_partitioner)
		}
		time.Sleep(5 * time.Second)

		atomic.StoreInt32(&done_clients, 1)     // tell clients to quit
		atomic.StoreInt32(&done_partitioner, 1) // tell partitioner to quit
		DPrintf("LMH done set \n")

		if partitions {
			// log.Printf("wait for partitioner\n")
			<-ch_partitioner
			// reconnect network and submit a request. A client may
			// have submitted a request in a minority.  That request
			// won't return until that server discovers a new term
			// has started.
			cfg.ConnectAll()
			// wait for a while so that we have a new term
			time.Sleep(electionTimeout)
		}

		if crash {
			 log.Printf("shutdown servers\n")
			for i := 0; i < nservers; i++ {
				cfg.ShutdownServer(i)
				log.Printf("shutdown servers %v\n", i)
			}
			// Wait for a while for servers to shutdown, since
			// shutdown isn't a real crash and isn't instantaneous
			time.Sleep(electionTimeout)
			 log.Printf("restart servers\n")
			// crash and re-start all
			for i := 0; i < nservers; i++ {
				log.Printf("start%v\n", i)
				cfg.StartServer(i)
			}
			log.Printf("connect all\n")
			cfg.ConnectAll()
		}

		 log.Printf("wait for clients\n")
		for i := 0; i < nclients; i++ {
			 log.Printf("read from clients %d\n", i)
			j := <-clnts[i]
			// if j < 10 {
			// 	log.Printf("Warning: client %d managed to perform only %d put operations in 1 sec?\n", i, j)
			// }
			key := strconv.Itoa(i)
			 log.Printf("Check %v for client %d\n", j, i)
			v := Get(cfg, ck, key)
			checkClntAppends(t, i, v, j)
		}

		if maxraftstate > 0 {
			// Check maximum after the servers have processed all client
			// requests and had time to checkpoint.
			if cfg.LogSize() > 2*maxraftstate {
				t.Fatalf("logs were not trimmed (%v > 2*%v)", cfg.LogSize(), maxraftstate)
			}
		}
		log.Printf("LLL Ok Done!!\n")
	}

	cfg.end()
}

// similar to GenericTest, but with clients doing random operations (and using a
// linearizability checker)
func GenericTestLinearizability(t *testing.T, part string, nclients int, nservers int, unreliable bool, crash bool, partitions bool, maxraftstate int) {

	title := "Test: "
	if unreliable {
		// the network drops RPC requests and replies.
		title = title + "unreliable net, "
	}
	if crash {
		// peers re-start, and thus persistence must work.
		title = title + "restarts, "
	}
	if partitions {
		// the network may partition
		title = title + "partitions, "
	}
	if maxraftstate != -1 {
		title = title + "snapshots, "
	}
	if nclients > 1 {
		title = title + "many clients"
	} else {
		title = title + "one client"
	}
	title = title + ", linearizability checks (" + part + ")" // 3A or 3B

	cfg := make_config(t, nservers, unreliable, maxraftstate)
	defer cfg.cleanup()

	cfg.begin(title)

	begin := time.Now()
	var operations []linearizability.Operation
	var opMu sync.Mutex

	done_partitioner := int32(0)
	done_clients := int32(0)
	ch_partitioner := make(chan bool)
	clnts := make([]chan int, nclients)
	for i := 0; i < nclients; i++ {
		clnts[i] = make(chan int)
	}
	for i := 0; i < 3; i++ {
		// log.Printf("Iteration %v\n", i)
		atomic.StoreInt32(&done_clients, 0)
		atomic.StoreInt32(&done_partitioner, 0)
		go spawn_clients_and_wait(t, cfg, nclients, func(cli int, myck *Clerk, t *testing.T) {
			j := 0
			defer func() {
				clnts[cli] <- j
			}()
			for atomic.LoadInt32(&done_clients) == 0 {
				key := strconv.Itoa(rand.Int() % nclients)
				nv := "x " + strconv.Itoa(cli) + " " + strconv.Itoa(j) + " y"
				var inp linearizability.KvInput
				var out linearizability.KvOutput
				start := int64(time.Since(begin))
				if (rand.Int() % 1000) < 500 {
					Append(cfg, myck, key, nv)
					inp = linearizability.KvInput{Op: 2, Key: key, Value: nv}
					j++
				} else if (rand.Int() % 1000) < 100 {
					Put(cfg, myck, key, nv)
					inp = linearizability.KvInput{Op: 1, Key: key, Value: nv}
					j++
				} else {
					v := Get(cfg, myck, key)
					inp = linearizability.KvInput{Op: 0, Key: key}
					out = linearizability.KvOutput{Value: v}
				}
				end := int64(time.Since(begin))
				op := linearizability.Operation{Input: inp, Call: start, Output: out, Return: end}
				opMu.Lock()
				operations = append(operations, op)
				opMu.Unlock()
			}
		})

		if partitions {
			// Allow the clients to perform some operations without interruption
			time.Sleep(1 * time.Second)
			go partitioner(t, cfg, ch_partitioner, &done_partitioner)
		}
		time.Sleep(5 * time.Second)

		atomic.StoreInt32(&done_clients, 1)     // tell clients to quit
		atomic.StoreInt32(&done_partitioner, 1) // tell partitioner to quit

		if partitions {
			// log.Printf("wait for partitioner\n")
			<-ch_partitioner
			// reconnect network and submit a request. A client may
			// have submitted a request in a minority.  That request
			// won't return until that server discovers a new term
			// has started.
			cfg.ConnectAll()
			// wait for a while so that we have a new term
			time.Sleep(electionTimeout)
		}

		if crash {
			// log.Printf("shutdown servers\n")
			for i := 0; i < nservers; i++ {
				cfg.ShutdownServer(i)
			}
			// Wait for a while for servers to shutdown, since
			// shutdown isn't a real crash and isn't instantaneous
			time.Sleep(electionTimeout)
			// log.Printf("restart servers\n")
			// crash and re-start all
			for i := 0; i < nservers; i++ {
				cfg.StartServer(i)
			}
			cfg.ConnectAll()
		}

		// wait for clients.
		for i := 0; i < nclients; i++ {
			<-clnts[i]
		}

		if maxraftstate > 0 {
			// Check maximum after the servers have processed all client
			// requests and had time to checkpoint.
			if cfg.LogSize() > 2*maxraftstate {
				t.Fatalf("logs were not trimmed (%v > 2*%v)", cfg.LogSize(), maxraftstate)
			}
		}
	}

	cfg.end()

	// log.Printf("Checking linearizability of %d operations", len(operations))
	// start := time.Now()
	ok := linearizability.CheckOperationsTimeout(linearizability.KvModel(), operations, linearizabilityCheckTimeout)
	// dur := time.Since(start)
	// log.Printf("Linearizability check done in %s; result: %t", time.Since(start).String(), ok)
	if !ok {
		t.Fatal("history is not linearizable")
	}
}
func TestBasic3A(t *testing.T) {
	if Test3A == false {
		return
	}
	// Test: one client (3A) ...
	GenericTest(t, "3A", 1, false, false, false, -1)
}

func TestConcurrent3A(t *testing.T) {
	if Test3A == false {
		return
	}
	// Test: many clients (3A) ...
	GenericTest(t, "3A", 5, false, false, false, -1)
}

func TestUnreliable3A(t *testing.T) {
	if Test3A == false {
		return
	}
	// Test: unreliable net, many clients (3A) ...
	GenericTest(t, "3A", 5, true, false, false, -1)
}
// TODO
func TestUnreliableOneKey3A(t *testing.T) {
	const nservers = 3
	cfg := make_config(t, nservers, true, -1)
	defer cfg.cleanup()

	ck := cfg.makeClient(cfg.All())

	cfg.begin("Test: concurrent append to same key, unreliable (3A)")

	Put(cfg, ck, "k", "")

	const nclient = 5
	const upto = 10
	spawn_clients_and_wait(t, cfg, nclient, func(me int, myck *Clerk, t *testing.T) {
		n := 0
		for n < upto {
			Append(cfg, myck, "k", "x "+strconv.Itoa(me)+" "+strconv.Itoa(n)+" y")
			n++
		}
	})

	var counts []int
	for i := 0; i < nclient; i++ {
		counts = append(counts, upto)
	}

	vx := Get(cfg, ck, "k")
	checkConcurrentAppends(t, vx, counts)

	cfg.end()
}

// Submit a request in the minority partition and check that the requests
// doesn't go through until the partition heals.  The leader in the original
// network ends up in the minority partition.
func TestOnePartition3A(t *testing.T) {
	const nservers = 5
	cfg := make_config(t, nservers, false, -1)
	defer cfg.cleanup()
	ck := cfg.makeClient(cfg.All())

	Put(cfg, ck, "1", "13")

	cfg.begin("Test: progress in majority (3A)")

	p1, p2 := cfg.make_partition()
	cfg.partition(p1, p2)

	ckp1 := cfg.makeClient(p1)  // connect ckp1 to p1
	ckp2a := cfg.makeClient(p2) // connect ckp2a to p2
	ckp2b := cfg.makeClient(p2) // connect ckp2b to p2

	Put(cfg, ckp1, "1", "14")
	check(cfg, t, ckp1, "1", "14")

	cfg.end()

	done0 := make(chan bool)
	done1 := make(chan bool)

	cfg.begin("Test: no progress in minority (3A)")
	go func() {
		Put(cfg, ckp2a, "1", "15")
		done0 <- true
	}()
	go func() {
		Get(cfg, ckp2b, "1") // different clerk in p2
		done1 <- true
	}()

	select {
	case <-done0:
		t.Fatalf("Put in minority completed")
	case <-done1:
		t.Fatalf("Get in minority completed")
	case <-time.After(time.Second):
	}

	check(cfg, t, ckp1, "1", "14")
	Put(cfg, ckp1, "1", "16")
	check(cfg, t, ckp1, "1", "16")

	cfg.end()

	cfg.begin("Test: completion after heal (3A)")

	cfg.ConnectAll()
	cfg.ConnectClient(ckp2a, cfg.All())
	cfg.ConnectClient(ckp2b, cfg.All())

	time.Sleep(electionTimeout)

	select {
	case <-done0:
	case <-time.After(30 * 100 * time.Millisecond):
		t.Fatalf("Put did not complete")
	}

	select {
	case <-done1:
	case <-time.After(30 * 100 * time.Millisecond):
		t.Fatalf("Get did not complete")
	default:
	}

	check(cfg, t, ck, "1", "15")

	cfg.end()
}

func TestManyPartitionsOneClient3A(t *testing.T) {
	// Test: partitions, one client (3A) ...
	GenericTest(t, "3A", 1, false, false, true, -1)
}

func TestManyPartitionsManyClients3A(t *testing.T) {
	// Test: partitions, many clients (3A) ...
	GenericTest(t, "3A", 5, false, false, true, -1)
}

func TestPersistOneClient3A(t *testing.T) {
	// Test: restarts, one client (3A) ...
	GenericTest(t, "3A", 1, false, true, false, -1)
}

func TestPersistConcurrent3A(t *testing.T) {
	// Test: restarts, many clients (3A) ...
	GenericTest(t, "3A", 5, false, true, false, -1)
}

func TestPersistConcurrentUnreliable3A(t *testing.T) {
	// Test: unreliable net, restarts, many clients (3A) ...
	GenericTest(t, "3A", 5, true, true, false, -1)
}

func TestPersistPartition3A(t *testing.T) {
	// Test: restarts, partitions, many clients (3A) ...
	GenericTest(t, "3A", 5, false, true, true, -1)
}

func TestPersistPartitionUnreliable3A(t *testing.T) {
	// Test: unreliable net, restarts, partitions, many clients (3A) ...
	GenericTest(t, "3A", 5, true, true, true, -1)
}

func TestPersistPartitionUnreliableLinearizable3A(t *testing.T) {
	// Test: unreliable net, restarts, partitions, linearizability checks (3A) ...
	GenericTestLinearizability(t, "3A", 15, 7, true, true, true, -1)
}

//
// if one server falls behind, then rejoins, does it
// recover by using the InstallSnapshot RPC?
// also checks that majority discards committed log entries
// even if minority doesn't respond.
//
func TestSnapshotRPC3B(t *testing.T) {
	const nservers = 3
	maxraftstate := 1000
	cfg := make_config(t, nservers, false, maxraftstate)
	defer cfg.cleanup()

	ck := cfg.makeClient(cfg.All())

	cfg.begin("Test: InstallSnapshot RPC (3B)")

	Put(cfg, ck, "a", "A")
	check(cfg, t, ck, "a", "A")

	// a bunch of puts into the majority partition.
	cfg.partition([]int{0, 1}, []int{2})
	{
		ck1 := cfg.makeClient([]int{0, 1})
		for i := 0; i < 50; i++ {
			Put(cfg, ck1, strconv.Itoa(i), strconv.Itoa(i))
		}
		time.Sleep(electionTimeout)
		Put(cfg, ck1, "b", "B")
	}

	DPrintf("Oh 已经提交Put 53\n")

	// check that the majority partition has thrown away
	// most of its log entries.
	if cfg.LogSize() > 2*maxraftstate {
		t.Fatalf("logs were not trimmed (%v > 2*%v)", cfg.LogSize(), maxraftstate)
	}

	// now make group that requires participation of
	// lagging server, so that it has to catch up.
	cfg.partition([]int{0, 2}, []int{1})
	{
		ck1 := cfg.makeClient([]int{0, 2})
		DPrintf("OKK Ready\n")
		Put(cfg, ck1, "c", "C")
		DPrintf("OKK 已经提交Put 54\n")
		Put(cfg, ck1, "d", "D")
		DPrintf("OKK 已经提交Put 55\n")
		check(cfg, t, ck1, "a", "A")
		check(cfg, t, ck1, "b", "B")
		check(cfg, t, ck1, "1", "1")
		check(cfg, t, ck1, "49", "49")
		DPrintf("OKK 已经Get 59\n")
	}
	DPrintf("Oh 分区开始合并\n")
	// now everybody
	cfg.partition([]int{0, 1, 2}, []int{})

	Put(cfg, ck, "e", "E")
	check(cfg, t, ck, "c", "C")
	check(cfg, t, ck, "e", "E")
	check(cfg, t, ck, "1", "1")

	cfg.end()
}

// are the snapshots not too huge? 500 bytes is a generous bound for the
// operations we're doing here.
func TestSnapshotSize3B(t *testing.T) {
	const nservers = 3
	maxraftstate := 1000
	maxsnapshotstate := 500
	cfg := make_config(t, nservers, false, maxraftstate)
	defer cfg.cleanup()

	ck := cfg.makeClient(cfg.All())

	cfg.begin("Test: snapshot size is reasonable (3B)")

	for i := 0; i < 200; i++ {
		Put(cfg, ck, "x", "0")
		check(cfg, t, ck, "x", "0")
		Put(cfg, ck, "x", "1")
		check(cfg, t, ck, "x", "1")
	}

	// check that servers have thrown away most of their log entries
	if cfg.LogSize() > 2*maxraftstate {
		t.Fatalf("logs were not trimmed (%v > 2*%v)", cfg.LogSize(), maxraftstate)
	}

	// check that the snapshots are not unreasonably large
	if cfg.SnapshotSize() > maxsnapshotstate {
		t.Fatalf("snapshot too large (%v > %v)", cfg.SnapshotSize(), maxsnapshotstate)
	}

	cfg.end()
}

func TestSnapshotRecover3B(t *testing.T) {
	// Test: restarts, snapshots, one client (3B) ...
	GenericTest(t, "3B", 1, false, true, false, 1000)
}

func TestSnapshotRecoverManyClients3B(t *testing.T) {
	// Test: restarts, snapshots, many clients (3B) ...
	GenericTest(t, "3B", 20, false, true, false, 1000)
}

func TestSnapshotUnreliable3B(t *testing.T) {
	// Test: unreliable net, snapshots, many clients (3B) ...
	GenericTest(t, "3B", 5, true, false, false, 1000)
}

func TestSnapshotUnreliableRecover3B(t *testing.T) {
	// Test: unreliable net, restarts, snapshots, many clients (3B) ...
	GenericTest(t, "3B", 5, true, true, false, 1000)
}

func TestSnapshotUnreliableRecoverConcurrentPartition3B(t *testing.T) {
	// Test: unreliable net, restarts, partitions, snapshots, many clients (3B) ...
	GenericTest(t, "3B", 5, true, true, true, 1000)
}

func TestSnapshotUnreliableRecoverConcurrentPartitionLinearizable3B(t *testing.T) {
	// Test: unreliable net, restarts, partitions, snapshots, linearizability checks (3B) ...
	GenericTestLinearizability(t, "3B", 15, 7, true, true, true, 1000)
}
