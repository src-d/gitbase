package server

import (
	"flag"
	"net"
	"os"

	"gopkg.in/bblfsh/sdk.v2/driver"
	"gopkg.in/src-d/go-errors.v1"
)

var (
	ErrInvalidLogger       = errors.NewKind("invalid logger configuration")
	ErrUnsupportedLanguage = errors.NewKind("unsupported language: %q")
)

var (
	network *string
	address *string
	verbose *string
	logs    struct {
		level  *string
		format *string
		fields *string
	}
)

// Server is a grpc server for the communication with the driver.
type Server struct {
	*GRPCServer
	// Logger a logger to be used by the server.
	Logger Logger

	d driver.DriverModule
}

// NewServer returns a new server for a given Driver.
func NewServer(d driver.DriverModule) *Server {
	return &Server{d: d, GRPCServer: NewGRPCServer(d)}
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

	return s.Serve(l)
}

func (s *Server) initialize() error {
	s.initializeFlags()
	if err := s.initializeLogger(); err != nil {
		return err
	}
	m, err := s.d.Manifest()
	if err != nil {
		return err
	}

	build := "unknown"
	if m.Build != nil {
		build = m.Build.Format("2006-01-02T15:04:05Z")
	}
	s.Logger.Infof("%s-driver version: %s (build: %s)",
		m.Language,
		m.Version,
		build,
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
	logs.level = cmd.String("log-level", defaultVerbose, "log level: panic, fatal, error, warning, info, debug.")
	logs.format = cmd.String("log-format", defaultFormat, "format of the logs: text or json.")
	logs.fields = cmd.String("log-fields", "", "extra fields to add to every log line in json format.")

	cmd.Parse(os.Args[1:])
}

func (s *Server) initializeLogger() error {
	f := LoggerFactory{
		Level:  *logs.level,
		Format: *logs.format,
		Fields: *logs.fields,
	}

	var err error
	s.Logger, err = f.New()
	if err != nil {
		return ErrInvalidLogger.Wrap(err)
	}

	return nil
}
