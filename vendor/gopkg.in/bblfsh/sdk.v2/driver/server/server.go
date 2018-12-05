package server

import (
	"flag"
	"io"
	"net"
	"os"

	jaegercfg "github.com/uber/jaeger-client-go/config"
	"google.golang.org/grpc"

	cmdutil "gopkg.in/bblfsh/sdk.v2/cmd"
	"gopkg.in/bblfsh/sdk.v2/driver"
	"gopkg.in/src-d/go-errors.v1"
)

var (
	// ErrInvalidTracer is returned by the driver server when the logger configuration is wrong.
	ErrInvalidLogger = errors.NewKind("invalid logger configuration")
	// ErrInvalidTracer is returned by the driver server when the tracing configuration is wrong.
	ErrInvalidTracer = errors.NewKind("invalid tracer configuration")
	// ErrUnsupportedLanguage is returned by the language server if the language in the request
	// is not supported by the driver.
	ErrUnsupportedLanguage = errors.NewKind("unsupported language: %q")
)

var (
	network        *string
	address        *string
	verbose        *string
	maxMessageSize *int
	logs           struct {
		level  *string
		format *string
		fields *string
	}
)

// Server is a grpc server for the communication with the driver.
type Server struct {
	grpc *grpc.Server
	// Logger a logger to be used by the server.
	Logger Logger

	d driver.DriverModule

	// closers is a list of things to be closed
	// TODO: proper driver shutdown logic; it's unused right now
	closers []io.Closer
}

// NewServer returns a new server for a given Driver.
func NewServer(d driver.DriverModule) *Server {
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

	return s.grpc.Serve(l)
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
	if err := s.initializeTracing(m.Language + "-driver"); err != nil {
		return err
	}

	grpcOpts, err := cmdutil.GRPCSizeOptions(*maxMessageSize)
	if err != nil {
		s.Logger.Errorf(err.Error())
		os.Exit(1)
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
	s.grpc = NewGRPCServer(s.d, grpcOpts...)
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
	maxMessageSize = cmdutil.FlagMaxGRPCMsgSizeMB(cmd)

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

func (s *Server) initializeTracing(serviceName string) error {
	c, err := jaegercfg.FromEnv()
	if err != nil {
		return ErrInvalidTracer.Wrap(err)
	}
	closer, err := c.InitGlobalTracer(serviceName)
	if err != nil {
		return ErrInvalidTracer.Wrap(err)
	}
	s.closers = append(s.closers, closer)
	return nil
}
