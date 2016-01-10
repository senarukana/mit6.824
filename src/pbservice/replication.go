package pbservice

import (
	"golang.org/x/net/context"
	"log"
	"runtime"
	"time"
)

type Replication struct {
	isRunning  bool
	me         string
	backupAddr string
	repliChan  chan *RepliOperationArgs
	genChan    chan *RepliOperationArgs
	cancel     context.CancelFunc
	synced     bool

	backupData map[string]*Value
}

func NewReplication(me string) *Replication {
	return &Replication{
		me: me,
	}
}

func (rp *Replication) replication(ctx context.Context) {
	client := MakeReplicationClerk(rp.backupAddr)
	opDoneChan := make(chan bool)
	nonBlockOp := func(args *RepliOperationArgs) {
		var reply PutAppendReply
		ok := client.Replicate(args.PutAppendArgs, &reply)
		if !ok || reply.Err != "" {
			opDoneChan <- false
			return
		}
		if args.doneChan != nil {
			args.doneChan <- true
		}
		opDoneChan <- true
	}
	for {
		select {
		case args := <-rp.genChan:
			var ok bool
			for !ok {
				go nonBlockOp(args)
				select {
				case ok = <-opDoneChan:
					if !ok {
						time.Sleep(time.Millisecond * 200)
					}
					continue
				case <-ctx.Done():
					log.Printf("replication %s is done: %s", rp.backupAddr, ctx.Err())
					return
				}
			}
		case <-ctx.Done():
			log.Printf("replication %s is done: %s", rp.backupAddr, ctx.Err())
			return
		}
	}
}

func (rp *Replication) generator(ctx context.Context) {
	for key, value := range rp.backupData {
		args := &RepliOperationArgs{
			PutAppendArgs: &PutAppendArgs{
				Key:       key,
				Value:     value.data,
				Token:     value.token,
				Operation: PUT_OPERATION,
			},
		}
		select {
		case rp.genChan <- args:
			// do nothing
		case <-ctx.Done():
			log.Printf("generator %s origin closed: %s", rp.backupAddr, ctx.Err())
			return
		}
	}
	rp.synced = true
	log.Printf("%s synced\n", rp.backupAddr)
	for {
		select {
		case args := <-rp.repliChan:
			select {
			case rp.genChan <- args:
				// do nothing
			case <-ctx.Done():
				log.Printf("generator %s repli closed: %s", rp.backupAddr, ctx.Err())
				return
			}
		case <-ctx.Done():
			log.Printf("generator %s repli closed: %s", rp.backupAddr, ctx.Err())
			return
		}
	}
}

func (rp *Replication) cloneData(data map[string]*Value) {
	rp.backupData = make(map[string]*Value)
	for key, value := range data {
		rp.backupData[key] = value
	}
}

func (rp *Replication) start(data map[string]*Value, backupAddr string) chan *RepliOperationArgs {
	rp.stop()
	if backupAddr == "" {
		rp.synced = true
		return nil
	}
	log.Printf("begin replicating data to %s\n", backupAddr)
	rp.isRunning = true
	rp.repliChan = make(chan *RepliOperationArgs)
	rp.genChan = make(chan *RepliOperationArgs)
	rp.backupAddr = backupAddr

	ctx, cancel := context.WithCancel(context.Background())
	rp.cloneData(data)
	go rp.generator(ctx)
	go rp.replication(ctx)
	rp.cancel = cancel

	return rp.repliChan
}

func (rp *Replication) stop() {
	if rp.isRunning {
		log.Printf("backup %s stop\n", rp.backupAddr)
		rp.cancel()
		runtime.Gosched()
		rp.backupAddr = ""
		rp.synced = false
	}
}
