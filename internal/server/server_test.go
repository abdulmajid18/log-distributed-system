package server

import (
	"context"
	"net"
	"testing"

	api "github.com/abdulmajid18/log-distributed-system/api/v1"
	"github.com/abdulmajid18/log-distributed-system/internal/config"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
)

// func TestServer(t *testing.T) {
// 	for scenario, fn := range map[string]func(
// 		t *testing.T,
// 		client api.LogClient,
// 		config *Config,
// 	){
// 		"produce/consume a message to/from the log succeeeds": testProduceConsume,
// 		"produce/consume stream succeeds":                     testProduceConsumeStream,
// 		"consume past log boundary fails":                     testConsumePastBoundary,
// 	} {
// 		t.Run(scenario, func(t *testing.T) {
// 			client, config, teardown := setupTest(t, nil)
// 			defer teardown()
// 			fn(t, client, config)
// 		})
// 	}
// }

func TestServer(t *testing.T) {
	for scenario, fn := range map[string]func(
		t *testing.T,
		rootClient api.LogClient,
		nobodyClient api.LogClient,
		config *Config,
	){
		// ...
	} {
		t.Run(scenario, func(t *testing.T) {
			rootClient,
				nobodyClient,
				config,
				teardown := setupTest(t, nil)
			defer teardown()
			fn(t, rootClient, nobodyClient, config)
		})
	}
}

func setupTest(t *testing.T, fn func(*Config)) (
	client api.LogClient,
	cfg *Config,
	teardown func(),
) {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	// clientTLSConfig, err := config.SetupTLSConfig(config.TLSConfig{
	// 	CAFile: config.CAFile,
	// })
	// require.NoError(t, err)
	// clientCreds := credentials.NewTLS(clientTLSConfig)
	// cc, err := grpc.Dial(
	// 	l.Addr().String(),
	// 	grpc.WithTransportCredentials(clientCreds),
	// )
	// require.NoError(t, err)
	// client = api.NewLogClient(cc)

	// serverTLSConfig, err := config.SetupTLSConfig(config.TLSConfig{
	// 	CertFile:      config.ServerCertFile,
	// 	KeyFile:       config.ServerKeyFile,
	// 	CAFile:        config.CAFile,
	// 	ServerAddress: l.Addr().String(),
	// })
	// require.NoError(t, err)
	// serverCreds := credentials.NewTLS(serverTLSConfig)
	// dir, err := ioutil.TempDir("/home/rozz/go/src/github.com/abdulmajid18/log-distributed-system/internal/server/", "server-test")
	// require.NoError(t, err)
	// clog, err := log.NewLog(dir, log.Config{})
	// require.NoError(t, err)
	// cfg = &Config{
	// 	CommitLog: clog,
	// }
	// if fn != nil {
	// 	fn(cfg)
	// }
	// server, err := NewGRPCServer(cfg, grpc.Creds(serverCreds))
	// require.NoError(t, err)
	// go func() {
	// 	server.Serve(l)
	// }()
	// return client, cfg, func() {
	// 	server.Stop()
	// 	cc.Close()
	// 	l.Close()
	// }
	newClient := func(crtPath, keyPath string) (
		*grpc.ClientConn,
		api.LogClient,
		[]grpc.DialOption,
	) {
		tlsConfig, err := config.SetupTLSConfig(config.TLSConfig{
			CertFile: crtPath,
			KeyFile:  keyPath,
			CAFile:   config.CAFile,
			Server:   false,
		})
		require.NoError(t, err)
		tlsCreds := credentials.NewTLS(tlsConfig)
		opts := []grpc.DialOption{grpc.WithTransportCredentials(tlsCreds)}
		conn, err := grpc.Dial(l.Addr().String(), opts...)
		require.NoError(t, err)
		client := api.NewLogClient(conn)
		return conn, client, opts
	}
	var rootConn *grpc.ClientConn
	rootConn, rootClient, _ = newClient(
		config.RootClientCertFile,
		config.RootClientKeyFile,
	)
	var nobodyConn *grpc.ClientConn
	nobodyConn, nobodyClient, _ = newClient(
		config.NobodyClientCertFile,
		config.NobodyClientKeyFile,
	)
	return rootClient, nobodyClient, cfg, func() {
		server.Stop()
		rootConn.Close()
		nobodyConn.Close()
		l.Close()
		// clientOptions := []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
		// cc, err s= grpc.Dial(l.Addr().String(), clientOptions...)
		// require.NoError(t, err)
	}
}

func testProduceConsume(t *testing.T, client api.LogClient, config *Config) {
	ctx := context.Background()
	want := &api.Record{
		Value: []byte("hello world"),
	}
	produce, err := client.Produce(
		ctx,
		&api.ProduceRequest{
			Record: want,
		},
	)
	require.NoError(t, err)
	consume, err := client.Consume(ctx, &api.ConsumeRequest{
		Offset: produce.Offset,
	})
	require.NoError(t, err)
	require.Equal(t, want.Value, consume.Record.Value)
	require.Equal(t, want.Offset, consume.Record.Offset)
}

func testConsumePastBoundary(
	t *testing.T,
	client api.LogClient,
	config *Config,
) {
	ctx := context.Background()
	produce, err := client.Produce(ctx, &api.ProduceRequest{
		Record: &api.Record{
			Value: []byte("hello world"),
		},
	})
	require.NoError(t, err)
	consume, err := client.Consume(ctx, &api.ConsumeRequest{
		Offset: produce.Offset + 1,
	})
	if consume != nil {
		t.Fatal("consume not nil")
	}

	got := status.Code(err)
	want := status.Code(api.ErrOffsetOutOfRange{}.GRPCStatus().Err())
	if got != want {
		t.Fatalf("got err: %v, want: %v", got, want)
	}
}

func testProduceConsumeStream(
	t *testing.T,
	client api.LogClient,
	config *Config,
) {
	ctx := context.Background()
	records := []*api.Record{{
		Value:  []byte("first message"),
		Offset: 0,
	}, {
		Value:  []byte("second message"),
		Offset: 1,
	}}
	{
		stream, err := client.ProduceStream(ctx)
		require.NoError(t, err)
		for offset, record := range records {
			err = stream.Send(&api.ProduceRequest{
				Record: record,
			})
			require.NoError(t, err)
			res, err := stream.Recv()
			require.NoError(t, err)
			if res.Offset != uint64(offset) {
				t.Fatalf(
					"got offset: %d, want: %d",
					res.Offset,
					offset,
				)
			}
		}
	}
	{
		stream, err := client.ConsumeStream(
			ctx,
			&api.ConsumeRequest{Offset: 0},
		)
		require.NoError(t, err)
		for i, record := range records {
			res, err := stream.Recv()
			require.NoError(t, err)
			require.Equal(t, res.Record, &api.Record{
				Value:  record.Value,
				Offset: uint64(i),
			})
		}
	}
}
