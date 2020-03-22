# Raft
参照 6.824提取Raft相关代码


Raft		创建的多个Raft节点，自己主动通信选主  
			对于Leader收到每个任务都分配一个递增index, 并将操作存到log.只有Leader能接收Put/Append操作,以及最新的Get操作  
			定期将log发送给其他server(raft)节点  
			若log过多会产生快照, 若某个节点落下log过多，直接发送快照  
  
KVServer	客户端进行Get/Put/Append操作时会和相应Clerk进行交互，Clerk与KVServer(raft)节点们进行通信(寻找Leader等)  
			向Leader提交任务，commit完成后返回给客户端  
			若发现log日志大于给定阈值90%, 自动执行快照功能，保存clerk已经执行的最大seq，kv数据  



labrpc	目录  
labrpc.go		network的实现，如监听RPC-CALL并调整网络状态  


raft 目录  
raft.go			实现了Raft选主  
config.go		实现了Raft选主的环境，包括network的配置  
persister.go	实现了节点的持久化,log日志可以包含kv持久化的，只要applied对应成功即可


kvserver 目录  
client.go		实现了Clerk : 记录所有server的End, 为client的请求找到Leader并进行处理  
server.go		实现了Server的工作,对于请求扔到Raft，当返回时返回给Clerk  
config.go		创建network,使得kvserver等都能工作  

labgob 目录  
labgob.go		完成编码检查等工作  

