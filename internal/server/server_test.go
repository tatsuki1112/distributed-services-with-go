package server

import (
	"github.com/stretchr/testify/require"
	api "github.com/tatsuki1112/distributed-services-with-go/api/v1"
	"github.com/tatsuki1112/distributed-services-with-go/internal/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"net"
	"os"
	"testing"
)

func TestServer(t *testing.T) {
	for scenario, fn := range map[string]func(
		t *testing.T,
		client api.LogClient,
		config *Config, ){
		"produce/consume a message to/from th log succeeds": testProduceConsume,
		"produce/consume stream scceeds":                    testProduceConsumeStream,
		"consume past log boundary fails":                   testConsumePastBoundary,
	} {
		t.Run(scenario, func(t *testing.T) {
			client, config, teardown := setupTest(t, nil)
			defer teardown()
			fn(t, client, config)
		})
	}
}

func setupTest(t *testing.T, fn func(*Config)) (client api.LogClient, cfg *Config, teardown func()) {
	t.Helper()

	l, err := net.Listen("tcp", ":0")
	require.NoError(t, err)

	clientOpptions := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials())}

	cc, err := grpc.Dial(l.Addr().String(), clientOpptions...)

	require.NoError(t, err)

	dir, err := os.MkdirTemp("", "server-test")
	require.NoError(t, err)

	clog, err := log.NewLog(dir, log.Config{})
	require.NoError(t, err)

	cfg = &Config{
		CommitLog: clog,
	}

	if fn != nil {
		fn(cfg)
	}
	server, err := NewGrpcServer(cfg)
	require.NoError(t, err)

	go func() {
		server.Serve(l)
	}()

	client = api.NewLogClient(cc)

	return client, cfg, func() {
		cc.Close()
		server.Stop()
		l.Close()
		clog.Remove()
	}
}

