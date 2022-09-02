package log

import (
	"context"
	"sync"

	api "github.com/abdulmajid18/log-distributed-system/api/v1"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

type Replicator struct {
	DialOptions []grpc.DialOption
	LocalServer api.LogClient
	logger      *zap.Logger
	mu          sync.Mutex
	servers     map[string]chan struct{}
	closed      bool
	close       chan struct{}
}

//Join adds the server of the given address to list of servers to replicate from
// Runs the goroutine that runs the replication logic
func (r *Replicator) Join(name, addr string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.init()

	if r.closed {
		return nil
	}

	if _, ok := r.servers[name]; ok {
		//already replicating so return nil
		return nil
	}

	r.servers[name] = make(chan struct{})

	go r.replicate(addr, r.servers[name])

	return nil

}

//replicates logs received from serf member to the local server
func (r *Replicator) replicate(addr string, leave chan struct{}) {
	cc, err := grpc.Dial(addr, r.DialOptions...)

	if err != nil {
		r.logError(err, "failed to dial", addr)
	}

	defer cc.Close()
	client := api.NewLogClient(cc)

	ctx := context.Background()

	stream, err := client.ConsumeStream(ctx, &api.ConsumeRequest{Offset: 0})
	if err != nil {
		r.logError(err, "failed to consume", addr)
		return
	}

	records := make(chan *api.Record)

	// Receive all records and pass to records channel
	go func() {
		for {
			recv, err := stream.Recv()
			if err != nil {
				r.logError(err, "Failed to receive", addr)
				return
			}
			// Pass records received from the client to this chan
			records <- recv.Record
		}
	}()

	// Produce records from the records chan until the server leaves or fails
	for {
		select {
		case <-r.close:
			return
		case <-leave:
			return
		case records := <-records:
			_, err = r.LocalServer.Produce(ctx, &api.ProduceRequest{
				Record: records,
			})

			if err != nil {
				r.logError(err, "Failed to produce", addr)
			}
		}
	}
}

// Leave Handles server leaving by removing it from the
// list of servers to replicate from
func (r *Replicator) Leave(name string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.init()
	if _, ok := r.servers[name]; !ok {
		return nil
	}

	close(r.servers[name])
	delete(r.servers, name)

	return nil
}

//We use this init() helper to lazily initialize the server map. You should use lazy
//initialization to give your structs a useful zero value2 because having a useful
//zero value reduces the API’s size and complexity while maintaining the same
//functionality. Without a useful zero value, we’d either have to export a replicator
//constructor function for the user to call or export the servers field on
//the replicator struct for the user to set—making more API for the user to learn
//and then requiring them to write more code before they can use our struct.
func (r *Replicator) init() {
	if r.logger == nil {
		r.logger = zap.L().Named("replicator")
	}
	if r.servers == nil {
		r.servers = make(map[string]chan struct{})
	}

	if r.close == nil {
		r.close = make(chan struct{})
	}
}

//Close closes the replicator so that it
//doesn't replicate new servers that join the cluster,
//and it stops replicating existing servers by causing the replicate() goroutines
//to return
func (r *Replicator) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.init()

	if r.closed {
		return nil
	}
	r.closed = true
	close(r.close)
	return nil
}

func (r *Replicator) logError(err error, msg, addr string) {
	r.logger.Error(
		msg,
		zap.String("addr", addr),
		zap.Error(err),
	)
}
