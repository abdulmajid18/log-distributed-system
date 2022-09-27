package agent

import (
	"crypto/tls"
	"fmt"
	"net"
	"sync"

	api "github.com/abdulmajid18/log-distributed-system/api/v1"
	"github.com/abdulmajid18/log-distributed-system/discovery"
	"github.com/abdulmajid18/log-distributed-system/internal/auth"
	"github.com/abdulmajid18/log-distributed-system/internal/log"
	"github.com/abdulmajid18/log-distributed-system/internal/server"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

type Config struct {
	ServerTLSConfig *tls.Config
	PeerTLSConfig   *tls.Config
	DataDir         string
	BindAddr        string
	RPCPort         int
	NodeName        string
	StartJoinAddrs  []string
	ACLModelFile    string
	ACLModelPolicy  string
}

type Agent struct {
	Config
	log        *log.Log
	server     *grpc.Server
	membership *discovery.Membership
	replicator *log.Replicator

	shutdown     bool
	shutdowns    chan struct{}
	shutDownLock sync.Mutex
}

func (c Config) RPCAddr() (string, error) {
	host, _, err := net.SplitHostPort(c.BindAddr)
	if err != nil {
		return " ", err
	}
	return fmt.Sprintf("%s:%d", host, c.RPCPort), nil
}

func New(config Config) (*Agent, error) {
	agent := &Agent{
		Config:    config,
		shutdowns: make(chan struct{}),
	}

	setup := []func() error{
		agent.setupLogger,
		agent.setupLog,
		agent.setupServer,
		agent.setupMembership,
	}

	for _, fn := range setup {
		if err := fn(); err != nil {
			return nil, err
		}
	}

	return agent, nil
}

func (a *Agent) setupLogger() error {
	logger, err := zap.NewDevelopment()
	if err != nil {
		return err
	}

	zap.ReplaceGlobals(logger)
	return nil
}

func (a *Agent) setupLog() error {
	var err error
	a.log, err = log.NewLog(a.DataDir, log.Config{})
	return err
}

func (a *Agent) setupMembership() error {
	addr, err := a.RPCAddr()
	if err != nil {
		return err
	}
	var opts []grpc.DialOption
	if a.Config.PeerTLSConfig != nil {
		creds := credentials.NewTLS(a.Config.PeerTLSConfig)
		opts = append(opts, grpc.WithTransportCredentials(creds))
	}
	conn, err := grpc.Dial(addr, opts...)
	if err != nil {
		return err
	}
	client := api.NewLogClient(conn)

	replicator := &log.Replicator{
		LocalServer: client,
		DialOptions: opts,
	}

	a.membership, err = discovery.New(replicator, discovery.Config{
		NodeName: a.Config.NodeName,
		BindAddr: a.Config.BindAddr,
		Tags: map[string]string{
			"rpc_addr": addr,
		},
		StartJoinAddrs: a.Config.StartJoinAddrs,
	})

	return err

}

func (a *Agent) setupServer() error {
	authorizer := auth.New(a.ACLModelFile, a.ACLModelPolicy)

	config := &server.Config{
		CommitLog:  a.log,
		Authorizer: authorizer,
	}

	var opts []grpc.ServerOption

	if a.Config.ServerTLSConfig != nil {
		creds := credentials.NewTLS(a.Config.ServerTLSConfig)
		opts = append(opts, grpc.Creds(creds))
	}

	var err error
	a.server, err = server.NewGRPCServer(config, opts...)
	if err != nil {
		return err
	}

	addr, err := a.RPCAddr()
	if err != nil {
		return err
	}
	listen, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}

	go func() {
		err := a.server.Serve(listen)
		if err != nil {
			_ = a.Shutdown()
		}
	}()

	return err

}

func (a *Agent) Shutdown() error {
	a.shutDownLock.Lock()
	defer a.shutDownLock.Unlock()
	if a.shutdown {
		return nil
	}
	a.shutdown = true
	close(a.shutdowns)
	shutdown := []func() error{
		a.membership.Leave,
		a.replicator.Close,
		func() error {
			a.server.GracefulStop()
			return nil
		},
		a.log.Close,
	}
	for _, fn := range shutdown {
		if err := fn(); err != nil {
			return err
		}
	}
	return nil
}
