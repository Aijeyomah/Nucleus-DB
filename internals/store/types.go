package store

type PutOp struct {
	Key       string
	Value     string
	ClientID  string
	RequestID string
	Lamport   uint64 // when I implement multiple shards Might implement a logical clock for conflict resolution between different shards
}

func NewPutOp(key, value, clientId, requestId string) PutOp {
	return PutOp{
		Key:       key,
		Value:     value,
		ClientID:  clientId,
		RequestID: requestId,
	}
}
