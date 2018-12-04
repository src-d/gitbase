package cmd

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGRPCOptions_InvalidInput(t *testing.T) {
	// too small
	opts, err := GRPCSizeOptions(-1)
	require.Error(t, err, "a small value should not be applied")
	require.Nil(t, opts)

	// too big
	opts, err = GRPCSizeOptions(maxMsgSizeCapMB + 1)
	require.Error(t, err, "a big value should not be applied")
	require.Nil(t, opts)
}

func TestGRPCOptions_ValidInput(t *testing.T) {
	opts, err := GRPCSizeOptions(32)

	require.Nil(t, err)
	require.NotNil(t, opts)
	require.Len(t, opts, 2)
	// does not work as expected for Func values like grpc.ServerOption
	//require.Contains(t, opts, grpc.MaxRecvMsgSize(DefaulGRPCMaxSendRecvMsgSizeMB))
	//require.Contains(t, opts, grpc.MaxSendMsgSize(DefaulGRPCMaxSendRecvMsgSizeMB))
}
