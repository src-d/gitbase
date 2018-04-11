package driver

import (
	"flag"
	"net"
	"os"

	"gopkg.in/bblfsh/sdk.v1/protocol"
	"gopkg.in/bblfsh/sdk.v1/sdk/server"
	"gopkg.in/src-d/go-errors.v1"
)

var ErrInvalidLogger = errors.NewKind("invalid logger configuration")

var (
	network *string
	address *string
	verbose *string
	log     struct {
		level  *string
		format *string
		fields *string
	}
)

// Server is a grpc server for the communication with the driver.
type Server struct {
	server.Server
	// Logger a logger to be used by the server.
	Logger server.Logger

	d *Driver
}

// NewServer returns a new server for a given Driver.
func NewServer(d *Driver) *Server {
	return &Server{d: d}
}

// Start executes the binary driver and start to listen in the network and
// address defined by the args.
func (s *Server) Start() error {
	if err := s.initialize(); err != nil {
		return err
	}

	s.Logger.Debugf("executing native binary ...")
	if err := s.d.Start(); err != nil {
		return err
	}

	l, err := net.Listen(*network, *address)
	if err != nil {
		return err
	}

	s.Logger.Infof("server listening in %s (%s)", *address, *network)

	protocol.DefaultService = s.d

	return s.Serve(l)
}

func (s *Server) initialize() error {
	s.initializeFlags()
	if err := s.initializeLogger(); err != nil {
		return err
	}

	s.Logger.Infof("%s-driver version: %s (build: %s)",
		s.d.m.Language,
		s.d.m.Version,
		s.d.m.Build.Format("2006-01-02T15:04:05Z"),
	)

	return nil
}

func (s *Server) initializeFlags() {
	const (
		defaultNetwork = "tcp"
		defaultAddress = "0.0.0.0:9432"
		defaultVerbose = "info"
		defaultFormat  = "text"
	)

	cmd := flag.NewFlagSet("server", flag.ExitOnError)
	network = cmd.String("network", defaultNetwork, "network type: tcp, tcp4, tcp6, unix or unixpacket.")
	address = cmd.String("address", defaultAddress, "address to listen.")
	log.level = cmd.String("log-level", defaultVerbose, "log level: panic, fatal, error, warning, info, debug.")
	log.format = cmd.String("log-format", defaultFormat, "format of the logs: text or json.")
	log.fields = cmd.String("log-fields", "", "extra fields to add to every log line in json format.")

	cmd.Parse(os.Args[1:])
}

func (s *Server) initializeLogger() error {
	f := server.LoggerFactory{
		Level:  *log.level,
		Format: *log.format,
		Fields: *log.fields,
	}

	var err error
	s.Logger, err = f.New()
	if err != nil {
		return ErrInvalidLogger.Wrap(err)
	}

	return nil
}
