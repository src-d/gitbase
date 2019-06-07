package command

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/src-d/gitbase"
	"github.com/src-d/gitbase/internal/function"
	"github.com/src-d/gitbase/internal/rule"

	"github.com/opentracing/opentracing-go"
	"github.com/sirupsen/logrus"
	sqle "github.com/src-d/go-mysql-server"
	"github.com/src-d/go-mysql-server/auth"
	"github.com/src-d/go-mysql-server/server"
	"github.com/src-d/go-mysql-server/sql"
	"github.com/src-d/go-mysql-server/sql/analyzer"
	"github.com/src-d/go-mysql-server/sql/index/pilosa"
	"github.com/uber/jaeger-client-go/config"
	"gopkg.in/src-d/go-git.v4/plumbing/cache"
	"gopkg.in/src-d/go-vitess.v1/mysql"
)

const (
	ServerDescription = "Starts a gitbase server instance"
	ServerHelp        = ServerDescription + "\n\n" +
		"By default when gitbase encounters an error in a repository it\n" +
		"stops the query. With GITBASE_SKIP_GIT_ERRORS variable it won't\n" +
		"complain and just skip those rows or repositories."
	TracerServiceName = "gitbase"
)

// Server represents the `server` command of gitbase cli tool.
type Server struct {
	engine   *sqle.Engine
	pool     *gitbase.RepositoryPool
	userAuth auth.Auth

	Name          string         `long:"db" default:"gitbase" description:"Database name"`
	Version       string         // Version of the application.
	Directories   []string       `short:"d" long:"directories" description:"Path where the git repositories are located (standard and siva), multiple directories can be defined. Accepts globs."`
	Depth         int            `long:"depth" default:"1000" description:"load repositories looking at less than <depth> nested subdirectories."`
	Host          string         `long:"host" default:"localhost" description:"Host where the server is going to listen"`
	Port          int            `short:"p" long:"port" default:"3306" description:"Port where the server is going to listen"`
	User          string         `short:"u" long:"user" default:"root" description:"User name used for connection"`
	Password      string         `short:"P" long:"password" default:"" description:"Password used for connection"`
	UserFile      string         `short:"U" long:"user-file" env:"GITBASE_USER_FILE" default:"" description:"JSON file with credentials list"`
	ConnTimeout   int            `short:"t" long:"timeout" env:"GITBASE_CONNECTION_TIMEOUT" description:"Timeout in seconds used for connections"`
	IndexDir      string         `short:"i" long:"index" default:"/var/lib/gitbase/index" description:"Directory where the gitbase indexes information will be persisted." env:"GITBASE_INDEX_DIR"`
	CacheSize     cache.FileSize `long:"cache" default:"512" description:"Object cache size in megabytes" env:"GITBASE_CACHESIZE_MB"`
	Parallelism   uint           `long:"parallelism" description:"Maximum number of parallel threads per table. By default, it's the number of CPU cores. 0 means default, 1 means disabled."`
	DisableSquash bool           `long:"no-squash" description:"Disables the table squashing."`
	TraceEnabled  bool           `long:"trace" env:"GITBASE_TRACE" description:"Enables jaeger tracing"`
	ReadOnly      bool           `short:"r" long:"readonly" description:"Only allow read queries. This disables creating and deleting indexes as well. Cannot be used with --user-file." env:"GITBASE_READONLY"`
	SkipGitErrors bool           // SkipGitErrors disables failing when Git errors are found.
	DisableGit    bool           `long:"no-git" description:"disable the load of git standard repositories."`
	DisableSiva   bool           `long:"no-siva" description:"disable the load of siva files."`
	Verbose       bool           `short:"v" description:"Activates the verbose mode"`
	LogLevel      string         `long:"log-level" env:"GITBASE_LOG_LEVEL" choice:"info" choice:"debug" choice:"warning" choice:"error" choice:"fatal" default:"info" description:"logging level"`
}

type jaegerLogrus struct {
	*logrus.Entry
}

func (l *jaegerLogrus) Error(s string) {
	l.Entry.Error(s)
}

func NewDatabaseEngine(
	userAuth auth.Auth,
	version string,
	parallelism int,
	squash bool,
) *sqle.Engine {
	catalog := sql.NewCatalog()
	ab := analyzer.NewBuilder(catalog)

	if parallelism == 0 {
		parallelism = runtime.NumCPU()
	}

	if parallelism > 1 {
		ab = ab.WithParallelism(parallelism)
	}

	if squash {
		ab = ab.AddPostAnalyzeRule(rule.SquashJoinsRule, rule.SquashJoins)
	}

	a := ab.Build()
	engine := sqle.New(catalog, a, &sqle.Config{
		VersionPostfix: version,
		Auth:           userAuth,
	})

	return engine
}

// Execute starts a new gitbase server based on provided configuration, it
// honors the go-flags.Commander interface.
func (c *Server) Execute(args []string) error {
	if c.Verbose {
		logrus.SetLevel(logrus.DebugLevel)
	}

	// info is the default log level
	if c.LogLevel != "info" {
		level, err := logrus.ParseLevel(c.LogLevel)
		if err != nil {
			return fmt.Errorf("cannot parse log level: %s", err.Error())
		}
		logrus.SetLevel(level)
	}

	var err error
	if c.UserFile != "" {
		if c.ReadOnly {
			return fmt.Errorf("cannot use both --user-file and --readonly")
		}

		c.userAuth, err = auth.NewNativeFile(c.UserFile)
		if err != nil {
			return err
		}
	} else {
		permissions := auth.AllPermissions
		if c.ReadOnly {
			permissions = auth.ReadPerm
		}
		c.userAuth = auth.NewNativeSingle(c.User, c.Password, permissions)
	}

	c.userAuth = auth.NewAudit(c.userAuth, auth.NewAuditLog(logrus.StandardLogger()))
	if err := c.buildDatabase(); err != nil {
		logrus.WithField("error", err).Fatal("unable to initialize database engine")
		return err
	}

	auth := mysql.NewAuthServerStatic()
	auth.Entries[c.User] = []*mysql.AuthServerStaticEntry{
		{Password: c.Password},
	}

	var tracer opentracing.Tracer
	if c.TraceEnabled {
		cfg, err := config.FromEnv()
		if err != nil {
			logrus.WithField("error", err).
				Fatal("unable to read jaeger environment")
			return err
		}
		if cfg.ServiceName == "" {
			cfg.ServiceName = TracerServiceName
		}

		logger := &jaegerLogrus{logrus.WithField("subsystem", "jaeger")}

		closer, err := cfg.InitGlobalTracer(cfg.ServiceName, config.Logger(logger))
		if err != nil {
			logrus.WithField("error", err).Fatal("unable to initialize global tracer")
			return err
		}

		tracer = opentracing.GlobalTracer()
		defer closer.Close()

		logrus.Info("tracing enabled")
	}

	hostString := net.JoinHostPort(c.Host, strconv.Itoa(c.Port))
	timeout := time.Duration(c.ConnTimeout) * time.Second
	s, err := server.NewServer(
		server.Config{
			Protocol:         "tcp",
			Address:          hostString,
			Auth:             c.userAuth,
			Tracer:           tracer,
			ConnReadTimeout:  timeout,
			ConnWriteTimeout: timeout,
		},
		c.engine,
		gitbase.NewSessionBuilder(c.pool,
			gitbase.WithSkipGitErrors(c.SkipGitErrors),
		),
	)
	if err != nil {
		return err
	}

	logrus.Infof("server started and listening on %s:%d", c.Host, c.Port)
	return s.Start()
}

func (c *Server) buildDatabase() error {
	if c.engine == nil {
		c.engine = NewDatabaseEngine(
			c.userAuth,
			c.Version,
			int(c.Parallelism),
			!c.DisableSquash,
		)
	}

	c.pool = gitbase.NewRepositoryPool(c.CacheSize * cache.MiByte)

	if err := c.addDirectories(); err != nil {
		return err
	}

	c.engine.AddDatabase(gitbase.NewDatabase(c.Name, c.pool))
	c.engine.AddDatabase(sql.NewInformationSchemaDatabase(c.engine.Catalog))
	c.engine.Catalog.SetCurrentDatabase(c.Name)
	logrus.WithField("db", c.Name).Debug("registered database to catalog")

	c.engine.Catalog.MustRegister(function.Functions...)
	logrus.Debug("registered all available functions in catalog")

	if err := c.registerDrivers(); err != nil {
		return err
	}

	if !c.DisableSquash {
		logrus.Info("squash tables rule is enabled")
	} else {
		logrus.Warn("squash tables rule is disabled")
	}

	return c.engine.Init()
}

func (c *Server) registerDrivers() error {
	if err := os.MkdirAll(c.IndexDir, 0755); err != nil {
		return err
	}

	logrus.Debug("created index storage")

	c.engine.Catalog.RegisterIndexDriver(
		pilosa.NewDriver(filepath.Join(c.IndexDir, pilosa.DriverID)),
	)
	logrus.Debug("registered pilosa index driver")

	return nil
}

func (c *Server) addDirectories() error {
	if len(c.Directories) == 0 {
		logrus.Error("At least one folder should be provided.")
	}

	if c.DisableGit && c.DisableSiva {
		logrus.Warn("The load of git repositories and siva files are disabled," +
			" no repository will be added.")

		return nil
	}

	if c.Depth < 1 {
		logrus.Warn("--depth flag set to a number less than 1," +
			" no repository will be added.")

		return nil
	}

	for _, directory := range c.Directories {
		if err := c.addDirectory(directory); err != nil {
			return err
		}
	}

	return nil
}

func (c *Server) addDirectory(directory string) error {
	matches, err := gitbase.PatternMatches(directory)
	if err != nil {
		return err
	}

	for _, match := range matches {
		if err := c.addMatch(directory, match); err != nil {
			logrus.WithFields(logrus.Fields{
				"path":  match,
				"error": err,
			}).Error("path couldn't be inspected")
		}
	}

	return nil
}

func (c *Server) addMatch(prefix, match string) error {
	root, err := filepath.Abs(match)
	if err != nil {
		return err
	}

	root, err = filepath.EvalSymlinks(root)
	if err != nil {
		return err
	}

	initDepth := strings.Count(root, string(os.PathSeparator))
	return filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			if os.IsPermission(err) {
				return filepath.SkipDir
			}
			return err
		}

		if info.IsDir() {
			if err := c.addIfGitRepo(prefix, path); err != nil {
				return err
			}

			depth := strings.Count(path, string(os.PathSeparator)) - initDepth
			if depth >= c.Depth {
				return filepath.SkipDir
			}

			return nil
		}

		if !c.DisableSiva &&
			info.Mode().IsRegular() &&
			gitbase.IsSivaFile(info.Name()) {
			id, err := gitbase.StripPrefix(prefix, path)
			if err != nil {
				return err
			}

			if err := c.pool.AddSivaFileWithID(id, path); err != nil {
				logrus.WithFields(logrus.Fields{
					"path":  path,
					"error": err,
				}).Error("repository could not be addded")

				return nil
			}

			logrus.WithField("path", path).Debug("repository added")
		}

		return nil
	})
}

func (c *Server) addIfGitRepo(prefix, path string) error {
	ok, err := gitbase.IsGitRepo(path)
	if err != nil {
		logrus.WithFields(logrus.Fields{
			"path":  path,
			"error": err,
		}).Error("path couldn't be inspected")

		return filepath.SkipDir
	}

	if ok {
		if !c.DisableGit {
			id, err := gitbase.StripPrefix(prefix, path)
			if err != nil {
				return err
			}

			if err := c.pool.AddGitWithID(id, path); err != nil {
				logrus.WithFields(logrus.Fields{
					"id":    id,
					"path":  path,
					"error": err,
				}).Error("repository could not be added")
			}

			logrus.WithField("path", path).Debug("repository added")
		}

		// either the repository is added or not, the path must be skipped
		return filepath.SkipDir
	}

	return nil
}
