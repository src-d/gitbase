package gitbase

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/connectivity"
)

func TestSessionBblfshClient(t *testing.T) {
	require := require.New(t)

	session := NewSession(nil, WithBblfshEndpoint(defaultBblfshEndpoint))
	cli, err := session.BblfshClient()
	require.NoError(err)
	require.NotNil(cli)
	require.Equal(connectivity.Ready, cli.GetState())
}
