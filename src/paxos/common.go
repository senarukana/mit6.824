package paxos

import "fmt"

type RequestNumber struct {
	Round    uint32
	ServerId uint32
}

func (n *RequestNumber) String() string {
	return fmt.Sprintf("round: %d, serverid: %d", n.Round, n.ServerId)
}

func (n *RequestNumber) cast() uint64 {
	return (uint64(n.Round) << 32) + uint64(n.ServerId)
}

func (n *RequestNumber) Less(o *RequestNumber) bool {
	if o == nil {
		return false
	}
	return n.cast() < o.cast()
}

type logItem struct {
	prepareNum  *RequestNumber
	acceptedNum *RequestNumber
	fate        Fate
	value       interface{}
}

type PrepareArgs struct {
	Seq int
	Num *RequestNumber
}

type PrepareReply struct {
	AcceptedNumber *RequestNumber
	AcceptedValue  interface{}
}

type AcceptArgs struct {
	Seq   int
	Num   *RequestNumber
	Value interface{}
}

type AcceptReply struct {
	PrepareNumber *RequestNumber
}

type DecidedArgs struct {
	Seq    int
	Num    *RequestNumber
	Value  interface{}
	Source int
	MaxSeq int64
}

type DecidedReply struct{}
