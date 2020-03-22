package labrpc

//import "fmt"
import "labgob"
import "bytes"
import "reflect"
import "sync"
import "log"
import "strings"
import "math/rand"
import "time"
import "sync/atomic"

type reqMsg struct {	// 发送的数据
	endname   interface{}	// 执行的节点
	svcMeth   string		// 方法名
	argsType  reflect.Type
	args	  []byte		// 参数
	replyCh	  chan replyMsg	// 接收返回的通道
}

type replyMsg struct {	// 接收的数据
	ok		bool		// 在网络(reliable)不可达或者server死掉情况返回false
	reply	[]byte
}

type ClientEnd struct {
	endname interface{}
	ch		chan	reqMsg	// 发送数据,已经连接到NewWork的endCh,所有RPC-Call都发送本通道
	done	chan	struct{}
}
// 请求 终端e 去执行
func (e *ClientEnd) Call(svcMeth string, args interface{}, reply interface{}) bool {
	req := reqMsg{}
	req.endname = e.endname
	req.svcMeth = svcMeth
	req.argsType = reflect.TypeOf(args)
	req.replyCh = make(chan replyMsg)

	qb := new(bytes.Buffer)
	qe := labgob.NewEncoder(qb)
	qe.Encode(args)
	req.args = qb.Bytes()

	select {
	case e.ch <- req:

	case <-e.done:
		return false;
	}

	rep := <-req.replyCh
	if rep.ok {
		rb := bytes.NewBuffer(rep.reply)
		rd := labgob.NewDecoder(rb)
		if err := rd.Decode(reply); err != nil {
			log.Fatalf("ClientEnd.Call():decode reply: %v\n",err)
		}
		return true
	} else {
		return false
	}
}

type Network struct {
	mu				sync.Mutex
	reliable		bool							// 网络是否可达
	longDelays		bool
	longReordering	bool							// 网络暂停一段时间
	ends			map[interface{}]*ClientEnd		// endname->ClientEnd的映射
	enabled			map[interface{}]bool			// 是否可到达
	servers			map[interface{}]*Server			// 节点->所有类型 如 node(1)-> (Raft,...)
	connections		map[interface{}]interface{}		// endname->servername(即 server编号)
	endCh			chan	reqMsg					// 接收请求管道
	done			chan	struct{}				// 终止管道
	count			int32							// RPC请求的个数
}
// 启动网络,同时监听请求管道和结束管道,相当于网络管理者，实时通过endCh监听RPC-Call
func MakeNetwork() *Network {
	rn := &Network{}
	rn.reliable = true
	rn.ends = map[interface{}]*ClientEnd{}
	rn.enabled = map[interface{}]bool{}
	rn.servers = map[interface{}]*Server{}
	rn.connections = map[interface{}](interface{}){}
	rn.endCh = make(chan reqMsg)
	rn.done = make(chan struct{})

	go func() {
		for {
			select {
			case xreq := <-rn.endCh:
				atomic.AddInt32(&rn.count, 1)
				go rn.ProcessReq(xreq)
			case <-rn.done:
				return
			}
		}
	}()

	return rn
}

func (rn *Network) Cleanup() {
	close(rn.done)
}

func (rn *Network) Reliable(yes bool) {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	rn.reliable = yes
}

func (rn *Network) LongReordering(yes bool) {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	rn.longReordering = yes
}

func (rn *Network) LongDelays(yes bool) {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	rn.longDelays = yes
}
// 获取endname信息
func (rn *Network) ReadEndnameInfo(endname interface{}) (enabled bool,
	servername interface{}, server *Server, reliable bool, longreordering bool,
) {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	enabled = rn.enabled[endname]
	servername = rn.connections[endname]
	if servername != nil {
		server = rn.servers[servername]
	}
	reliable = rn.reliable
	longreordering = rn.longReordering
	return
}

func (rn *Network) IsServerDead(endname interface{}, servername interface{}, server *Server) bool {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	if rn.enabled[endname] == false || rn.servers[servername] != server {
		return true
	}
	return false
}
// 接收到请求
func (rn *Network) ProcessReq(req reqMsg) {
	enabled, servername, server, reliable, longreordering := rn.ReadEndnameInfo(req.endname)

	if enabled && servername != nil && server != nil {
		if reliable == false {
			ms := (rand.Int() % 27)
			time.Sleep(time.Duration(ms) * time.Millisecond)
		}

		if reliable == false && (rand.Int()%1000) < 100 {
			req.replyCh <- replyMsg{false, nil}
			return
		}

		ech := make(chan replyMsg)
		go func() {
			// 启动线程去执行,完成放入管道
			r := server.dispatch(req)
			ech <- r
		}()

		var reply replyMsg
		replyOK := false
		serverDead := false
		for replyOK == false && serverDead == false {
			select {
			case reply = <-ech:
				replyOK = true
			case <-time.After(100 * time.Millisecond): // 超时,server也挂掉,数据直接扔掉
				serverDead = rn.IsServerDead(req.endname, servername, server)
				if serverDead {
					go func() {
						<-ech
					}()
				}
			}
		}

		serverDead = rn.IsServerDead(req.endname, servername, server)

		//fmt.Printf("endname = %v, servername = %v, server = %v is dead = %v replyOk = %v reliable = %v longreordering = %v\n",
		//					req.endname, servername, server, serverDead, replyOK, reliable, longreordering)

		if replyOK == false || serverDead == true {
			req.replyCh <- replyMsg{false, nil}
		} else if reliable == false && (rand.Int()%1000) < 100 {
			req.replyCh <- replyMsg{false, nil}
		} else if longreordering == true && rand.Intn(900) < 600 {
			ms := 200 + rand.Intn(1+rand.Intn(2000))
			time.AfterFunc(time.Duration(ms)*time.Millisecond, func() {
				req.replyCh <- reply
			})
		} else {
			req.replyCh <- reply	// 数据正常返回
		}
	} else {
		ms := 0
		if rn.longDelays {
			ms = (rand.Int() % 7000)
		} else {
			ms = (rand.Int()%100)
		}
		time.AfterFunc(time.Duration(ms)*time.Millisecond, func() {
			req.replyCh <- replyMsg{false, nil}
		})
	}
}

// 为endname 制作 ClientEnd, 将endCh绑定过去,可以进行通信
func (rn *Network) MakeEnd(endname interface{}) *ClientEnd {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	if _, ok := rn.ends[endname]; ok {
		log.Fatalf("MakeEnd %v already exists\n", endname)
	}

	e := &ClientEnd{}	// ClientEnd 初始化(endname, ch, done)
	e.endname = endname
	e.ch = rn.endCh
	e.done = rn.done
	rn.ends[endname] = e	// network记录映射 endname->clientEnd
	rn.enabled[endname] = false
	rn.connections[endname] = nil

	return e
}
// 某个节点的所有类型放
func (rn *Network) AddServer(servername interface{}, rs *Server) {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	rn.servers[servername] = rs
}

func (rn *Network) DeleteServer(servername interface{}) {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	rn.servers[servername] = nil
}
// 说明该endname(端口) 与 servername(server 编号)对应
func (rn *Network) Connect(endname interface{}, servername interface{}) {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	rn.connections[endname] = servername
}

func (rn *Network) Enable(endname interface{}, enabled bool) {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	rn.enabled[endname] = enabled
}

func (rn *Network) GetCount(servername interface{}) int {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	svr := rn.servers[servername]
	return svr.GetCount()
}

func (rn *Network) GetTotalCount() int {
	x := atomic.LoadInt32(&rn.count)
	return int(x)
}

type Server struct {
	mu			sync.Mutex
	services	map[string]*Service
	count		int					// RPC执行次数
}

func MakeServer() *Server {
	rs := &Server{}
	rs.services	= map[string]*Service{}
	return rs
}
// 将某个类型的Service放入
func (rs *Server) AddService(svc *Service) {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	rs.services[svc.name] = svc
}

// 分离如Call("Raft.AppendEntries",arg1,arg2)
func (rs *Server) dispatch(req reqMsg) replyMsg {
	rs.mu.Lock()
	rs.count += 1

	dot := strings.LastIndex(req.svcMeth, ".")
	serviceName := req.svcMeth[:dot]		// Raft
	methodName := req.svcMeth[dot+1:]		// AppendEntries

	service, ok := rs.services[serviceName]

	rs.mu.Unlock()

	if ok {
		return service.dispatch(methodName, req)
	} else {
		choices := []string{}
		for k, _ := range rs.services {
			choices = append(choices, k)
		}
		log.Fatalf("labrpc.Server.dispatch(): unknown service %v in %v.%v; expectiong one of %v\n",
			serviceName, serviceName, methodName, choices)
		return replyMsg{false, nil}
	}
}

func (rs *Server) GetCount() int {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	return rs.count
}

type Service struct {
	name string			// 该类型的名称
	rcvr reflect.Value	// 该类型的value
	typ  reflect.Type	// 该类型的type
	methods map[string]reflect.Method // 记录了需要的方法
}
// 获取rcvr类型的方法,以便于反射
func MakeService(rcvr interface{}) *Service {
	svc := &Service{}
	svc.typ = reflect.TypeOf(rcvr)
	svc.rcvr = reflect.ValueOf(rcvr)
	svc.name = reflect.Indirect(svc.rcvr).Type().Name()
	svc.methods = map[string]reflect.Method{}

	for m := 0; m <svc.typ.NumMethod(); m++ {
		method := svc.typ.Method(m)
		mtype := method.Type
		mname := method.Name

		if method.PkgPath != "" ||
			mtype.NumIn() != 3 ||
			mtype.In(2).Kind() != reflect.Ptr ||
			mtype.NumOut() != 0{
				// 要求输入是3个,输出是0个，第3个是Ptr类型
			} else {
				svc.methods[mname] = method // 将符合要求的方法放入map表中
		}
	}
	return svc
}

func (svc *Service) dispatch(methname string, req reqMsg) replyMsg {
	if method, ok := svc.methods[methname]; ok {
		args := reflect.New(req.argsType)

		ab := bytes.NewBuffer(req.args)
		ad := labgob.NewDecoder(ab)
		ad.Decode(args.Interface())

		replyType := method.Type.In(2)
		replyType = replyType.Elem()
		replyv := reflect.New(replyType)

		function := method.Func
		function.Call([]reflect.Value{svc.rcvr, args.Elem(), replyv})

		rb := new(bytes.Buffer)
		re := labgob.NewEncoder(rb)
		re.EncodeValue(replyv)

		return replyMsg{true, rb.Bytes()}
	} else {
		choices := []string{}
		for k, _ := range svc.methods {
			choices = append(choices, k)
		}
		log.Fatalf("labrpc.Service.dispatch(): unknown method %v in %v; expectiong one of %v\n",
			methname, req.svcMeth, choices)
		return replyMsg{false, nil}
	}
}
