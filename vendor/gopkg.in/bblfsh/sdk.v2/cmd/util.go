package cmd

// Contains CLI helpers, shared between bblfshd and drivers.

import (
	"flag"
	"fmt"

	"google.golang.org/grpc"
)

const (
	// MaxMsgSizeCLIName is name of the CLI flag to set max msg size.
	maxMsgSizeCLIName = "grpc-max-message-size"
	// MaxMsgSizeCLIDesc is description for the CLI flag to set max msg size.
	maxMsgSizeCLIDesc = "max. message size to send/receive to/from clients (in MB)"

	// DefaulGRPCMaxSendRecvMsgSizeMB is maximum msg size for gRPC in MB.
	defaulGRPCMaxSendRecvMsgSizeMB = 100
	maxMsgSizeCapMB                = 2048
)

// GRPCSizeOptions returns a slice of gRPC server options with the max
// message size the server can send/receive set.
// Error is returned if requested size is bigger than 2GB.
// It is intended to be shared by gRPC in bblfshd Server and Drivers.
func GRPCSizeOptions(sizeMB int) ([]grpc.ServerOption, error) {
	if sizeMB >= maxMsgSizeCapMB || sizeMB <= 0 {
		return nil, fmt.Errorf("%s=%d value should be in between 1 and %dMB",
			maxMsgSizeCLIName, sizeMB, maxMsgSizeCapMB-1)
	}

	sizeBytes := sizeMB * 1024 * 1024
	return []grpc.ServerOption{
		grpc.MaxRecvMsgSize(sizeBytes),
		grpc.MaxSendMsgSize(sizeBytes),
	}, nil
}

// FlagMaxGRPCMsgSizeMB sets the CLI configuation flag for max
// gRPC send/recive msg size.
func FlagMaxGRPCMsgSizeMB(fs *flag.FlagSet) *int {
	return fs.Int(maxMsgSizeCLIName, defaulGRPCMaxSendRecvMsgSizeMB, maxMsgSizeCLIDesc)
}
