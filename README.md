# Raft
参照 6.824提取Raft相关代码


Raft		创建的多个Raft节点，自己主动通信选主
KVServer	客户端进行Get/Put/Append操作时会和Clerk进行交互，Clerk与KVServer进行通信(寻找Leader等)完成后返回给客户端



labrpc	目录
labrpc.go		network的实现，如监听RPC-CALL并调整网络状态


raft 目录
raft.go			实现了Raft选主
config.go		实现了Raft选主的环境，包括network的配置
persister.go	实现了节点的持久化


kvserver 目录
client.go		实现了Clerk的工作，为client的请求找到Leader并进行处理
server.go		实现了Server的工作,对于请求扔到Raft，当返回时返回给Clerk
config.go		创建network,使得kvserver等都能工作

labgob 目录
labgob.go		完成编码检查等工作

