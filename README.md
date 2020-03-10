# Raft
参照 6.824提取Raft相关代码


Raft		创建的多个Raft节点，自己主动通信选主
KVServer	客户端进行Get/Put/Append操作时会和Clerk进行交互，Clerk与KVServer进行通信(寻找Leader等)完成后返回给客户端
