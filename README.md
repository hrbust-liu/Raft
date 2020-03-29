# Raft
参照 6.824提取Raft相关代码


Raft		创建的多个Raft节点，自己主动通信选主  
			对于Leader收到每个任务都分配一个递增index, 并将操作存到log.只有Leader能接收Put/Append操作,以及最新的Get操作  
			定期将log发送给其他server(raft)节点  
			若log过多会产生快照,保存kv与每个client接收的数量, 若某个节点落下log过多，直接发送快照  
  
KVServer	客户端进行Get/Put/Append操作时会和相应Clerk进行交互，Clerk与KVServer(raft)节点们进行通信(寻找Leader等)  
			向Leader提交任务，commit完成将操作执行到kv上，并记录该client完成的数量,结果返回给客户端  
			若发现log日志大于给定阈值90%, 自动执行快照功能，保存clerk已经执行的最大seq，kv数据  



labrpc	目录  
labrpc.go		network的实现，如监听RPC-CALL并调整网络状态  


raft 目录  
raft.go			实现了Raft选主，保存状态(term, voteFor, log)  
config.go		实现了Raft选主的环境，包括network的配置  
persister.go	实现了节点的持久化,分为快照(kv, cid-seq)和快照后的log日志(log中在启动后applied即可)  


kvserver 目录  
client.go		实现了Clerk : 记录所有server的End, 为client的请求找到Leader并进行处理  
server.go		实现了Server的工作,对于请求扔到Raft对应节点，成功返回给client, kv代表key-value, cid-seq代表当前kv对于每个client完成的操作个数, 定期让对应raft节点进行快照   
config.go		创建network,使得kvserver等都能工作  

labgob 目录  
labgob.go		完成编码检查等工作  

备注:  
raft.currentTerm大不能决定一切，对于单节点网络分区,term会无限上涨.  
commit大能决定一切,只有拥有最大commit日志的才可能成为下一界leader.  

go test 之前加入一下两句  
GOPATH=~/Raft/				// 项目放置的目录
export GOPATH				// 导入  
