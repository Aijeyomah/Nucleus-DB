Option A: Pure terminal

1. Start the control plane

`go run ./cmd/controlplane --addr :9000 --cluster-config ./cluster.yaml`

2.	Start shard-0 (3 nodes, separate shells)


## node-a

```
go run ./cmd/node --node-id=node-a --shard-id=0 --http-addr :8080 \
  --advertise-http http://localhost:8080 \
  --raft-addr 127.0.0.1:9001 \
  --raft-peers 127.0.0.1:9001,127.0.0.1:9002,127.0.0.1:9003 \
  --peer-http 127.0.0.1:9001=http://localhost:8080,127.0.0.1:9002=http://localhost:8081,127.0.0.1:9003=http://localhost:8082 \
  --raft-dir ./data/shard0/node-a \
  --control-plane http://localhost:9000
```

# node-b
```
go run ./cmd/node --node-id=node-b --shard-id=0 --http-addr :8081 \
  --advertise-http http://localhost:8081 \
  --raft-addr 127.0.0.1:9002 \
  --raft-peers 127.0.0.1:9001,127.0.0.1:9002,127.0.0.1:9003 \
  --peer-http 127.0.0.1:9001=http://localhost:8080,127.0.0.1:9002=http://localhost:8081,127.0.0.1:9003=http://localhost:8082 \
  --raft-dir ./data/shard0/node-b \
  --control-plane http://localhost:9000
```
## node-c
```
go run ./cmd/node --node-id=node-c --shard-id=0 --http-addr :8082 \
  --advertise-http http://localhost:8082 \
  --raft-addr 127.0.0.1:9003 \
  --raft-peers 127.0.0.1:9001,127.0.0.1:9002,127.0.0.1:9003 \
  --peer-http 127.0.0.1:9001=http://localhost:8080,127.0.0.1:9002=http://localhost:8081,127.0.0.1:9003=http://localhost:8082 \
  --raft-dir ./data/shard0/node-c \
  --control-plane http://localhost:9000
```

3.	Start shard-1 (3 nodes, separate shells; ports must match your cluster.yaml)

## node-d

```
go run ./cmd/node --node-id=node-d --shard-id=1 --http-addr :9080 \
  --advertise-http http://localhost:9080 \
  --raft-addr 127.0.0.1:9011 \
  --raft-peers 127.0.0.1:9011,127.0.0.1:9012,127.0.0.1:9013 \
  --peer-http 127.0.0.1:9011=http://localhost:9080,127.0.0.1:9012=http://localhost:9081,127.0.0.1:9013=http://localhost:9082 \
  --raft-dir ./data/shard1/node-d \
  --control-plane http://localhost:9000
```

## node-e

```
go run ./cmd/node --node-id=node-e --shard-id=1 --http-addr :9081 \
  --advertise-http http://localhost:9081 \
  --raft-addr 127.0.0.1:9012 \
  --raft-peers 127.0.0.1:9011,127.0.0.1:9012,127.0.0.1:9013 \
  --peer-http 127.0.0.1:9011=http://localhost:9080,127.0.0.1:9012=http://localhost:9081,127.0.0.1:9013=http://localhost:9082 \
  --raft-dir ./data/shard1/node-e \
  --control-plane http://localhost:9000
```


## node-f

```
go run ./cmd/node --node-id=node-f --shard-id=1 --http-addr :9082 \
  --advertise-http http://localhost:9082 \
  --raft-addr 127.0.0.1:9013 \
  --raft-peers 127.0.0.1:9011,127.0.0.1:9012,127.0.0.1:9013 \
  --peer-http 127.0.0.1:9011=http://localhost:9080,127.0.0.1:9012=http://localhost:9081,127.0.0.1:9013=http://localhost:9082 \
  --raft-dir ./data/shard1/node-f \
  --control-plane http://localhost:9000
```

4.	Check the cluster map from control plane
`curl -s localhost:9000/cluster/map  | jq`
`curl -s localhost:9000/cluster/live | jq`

5.	For each shard, find the leader, then write/read

Shard 0:
`curl -s localhost:8080/whoami | jq  # or :8081 / :8082 on others `

if `role != "leader"`, try the next node until you find leader, say `:8080`

`curl -s -X PUT localhost:8080/kv/user:1 -d 'active' | jq`

`curl -s localhost:8080/kv/user:1 | jq`

Shard 1:

`curl -s localhost:9080/whoami | jq   # find which one is leader among :9080/:9081/:9082`

`curl -s -X PUT localhost:9080/kv/order:9 -d 'paid' | jq`

`curl -s localhost:9080/kv/order:9 | jq`

# node-g
```
go run ./cmd/node --node-id=node-g --shard-id=2 --http-addr :9180 \
  --advertise-http http://localhost:9180 \
  --raft-addr 127.0.0.1:9021 \
  --raft-peers 127.0.0.1:9021,127.0.0.1:9022,127.0.0.1:9023 \
  --peer-http 127.0.0.1:9021=http://localhost:9180,127.0.0.1:9022=http://localhost:9181,127.0.0.1:9023=http://localhost:9182 \
  --raft-dir ./data/shard2/node-g \
  --control-plane http://localhost:9000
```

# node-h

```
go run ./cmd/node --node-id=node-h --shard-id=2 --http-addr :9181 \
  --advertise-http http://localhost:9181 \
  --raft-addr 127.0.0.1:9022 \
  --raft-peers 127.0.0.1:9021,127.0.0.1:9022,127.0.0.1:9023 \
  --peer-http 127.0.0.1:9021=http://localhost:9180,127.0.0.1:9022=http://localhost:9181,127.0.0.1:9023=http://localhost:9182 \
  --raft-dir ./data/shard2/node-h \
  --control-plane http://localhost:9000
```

```
# node-i
go run ./cmd/node --node-id=node-i --shard-id=2 --http-addr :9182 \
  --advertise-http http://localhost:9182 \
  --raft-addr 127.0.0.1:9023 \
  --raft-peers 127.0.0.1:9021,127.0.0.1:9022,127.0.0.1:9023 \
  --peer-http 127.0.0.1:9021=http://localhost:9180,127.0.0.1:9022=http://localhost:9181,127.0.0.1:9023=http://localhost:9182 \
  --raft-dir ./data/shard2/node-i \
  --control-plane http://localhost:9000
```

```curl -s -X POST http://localhost:9000/reshard/plan \
  -H 'Content-Type: application/json' \
  -d @plan.json | jq
```

`curl -s http://localhost:9000/reshard/status | jq   # if available in your CP`

```curl -s -X POST http://localhost:9000/reshard/cutover \
  -H 'Content-Type: application/json' \
  -d '{"epoch": 1}' | jq
```

## Optionally populate the KV store with data

### set your leader HTTP bases
`SHARD0=http://localhost:8081   # shard 0 leader`

`SHARD1=http://localhost:9080   # shard 1 leader`

### write 10 items to shard 0
```
for k in user:0010 user:0011 user:0012 user:0013 user:0014 user:0015 user:0016 user:0017 user:0018 user:0019; do
  curl -s -X PUT "$SHARD0/kv/$k" -d "value-for-$k"
done
```

### write 10 items to shard 1
```
for k in user:0001 user:0002 user:0003 user:0004 user:0005 user:0006 user:0007 user:0008 user:0009 user:0040; do
  curl -s -X PUT "$SHARD1/kv/$k" -d "value-for-$k"
done
```