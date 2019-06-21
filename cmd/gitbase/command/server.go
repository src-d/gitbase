package command

import (
	"fmt"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/src-d/gitbase"
	"github.com/src-d/gitbase/internal/function"
	"github.com/src-d/gitbase/internal/rule"

	"github.com/opentracing/opentracing-go"
	"github.com/sirupsen/logrus"
	"github.com/src-d/go-borges"
	"github.com/src-d/go-borges/libraries"
	"github.com/src-d/go-borges/plain"
	"github.com/src-d/go-borges/siva"
	sqle "github.com/src-d/go-mysql-server"
	"github.com/src-d/go-mysql-server/auth"
	"github.com/src-d/go-mysql-server/server"
	"github.com/src-d/go-mysql-server/sql"
	"github.com/src-d/go-mysql-server/sql/analyzer"
	"github.com/src-d/go-mysql-server/sql/index/pilosa"
	"github.com/uber/jaeger-client-go/config"
	"gopkg.in/src-d/go-billy.v4/osfs"
	"gopkg.in/src-d/go-git.v4/plumbing/cache"
	"vitess.io/vitess/go/mysql"
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

	rootLibrary  *libraries.Libraries
	plainLibrary *plain.Library
	sharedCache  cache.Object

	Name          string         `long:"db" default:"gitbase" description:"Database name"`
	Version       string         // Version of the application.
	Directories   []string       `short:"d" long:"directories" description:"Path where standard git repositories are located, multiple directories can be defined."`
	Format        string         `long:"format" default:"git" choice:"git" choice:"siva" description:"Library format"`
	Bucket        int            `long:"bucket" default:"2" description:"Bucketing level to use with siva libraries"`
	Bare          bool           `long:"bare" description:"Sets the library to use bare git repositories, used only with git format libraries"`
	NonBare       bool           `long:"non-bare" description:"Sets the library to use non bare git repositories, used only with git format libraries"`
	NonRooted     bool           `long:"non-rooted" description:"Disables treating siva files as rooted repositories"`
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

	if c.Bare && c.NonBare {
		return fmt.Errorf("cannot use both --bare and --non-bare")
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

	c.rootLibrary = libraries.New(libraries.Options{})
	c.pool = gitbase.NewRepositoryPool(c.CacheSize*cache.MiByte, c.rootLibrary)

	c.sharedCache = cache.NewObjectLRU(512 * cache.MiByte)

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
		logrus.Error("at least one folder should be provided.")
	}

	defaultBare := bareAuto
	switch {
	case c.Bare:
		defaultBare = bareOn
	case c.NonBare:
		defaultBare = bareOff
	}

	for _, d := range c.Directories {
		dir := directory{
			Path:   d,
			Format: c.Format,
			Bare:   defaultBare,
			Bucket: c.Bucket,
			Rooted: !c.NonRooted,
		}

		dir, err := parseDirectory(dir)
		if err != nil {
			return err
		}

		err = c.addDirectory(dir)
		if err != nil {
			return err
		}
	}

	repos, err := c.rootLibrary.Repositories(borges.ReadOnlyMode)
	if err != nil {
		return err
	}
	defer repos.Close()

	return repos.ForEach(func(r borges.Repository) error {
		id := r.ID().String()
		logrus.WithField("id", id).Debug("repository added")
		return r.Close()
	})
}

func (c *Server) addDirectory(d directory) error {
	if d.Format == "siva" {
		sivaOpts := siva.LibraryOptions{
			Transactional: true,
			RootedRepo:    d.Rooted,
			Cache:         c.sharedCache,
			Bucket:        d.Bucket,
			Performance:   true,
			RegistryCache: 100000,
		}

		lib, err := siva.NewLibrary(d.Path, osfs.New(d.Path), sivaOpts)
		if err != nil {
			return err
		}

		err = c.rootLibrary.Add(lib)
		if err != nil {
			return err
		}

		return nil
	}

	bare, err := discoverBare(d)
	if err != nil {
		return err
	}

	plainOpts := &plain.LocationOptions{
		Cache:       c.sharedCache,
		Performance: true,
		Bare:        bare,
	}

	if c.plainLibrary == nil {
		c.plainLibrary = plain.NewLibrary(borges.LibraryID("plain"))
		err := c.rootLibrary.Add(c.plainLibrary)
		if err != nil {
			return err
		}
	}

	loc, err := plain.NewLocation(
		borges.LocationID(d.Path),
		osfs.New(d.Path),
		plainOpts)
	if err != nil {
		return err
	}

	c.plainLibrary.AddLocation(loc)

	return nil
}

type bareOpt int

const (
	bareAuto bareOpt = iota
	bareOn
	bareOff
)

type directory struct {
	Path   string
	Format string
	Bucket int
	Rooted bool
	Bare   bareOpt
}

var (
	uriReg     = regexp.MustCompile(`^\w+:.*`)
	ErrInvalid = fmt.Errorf("invalid option")
)

func parseDirectory(dir directory) (directory, error) {
	d := dir.Path

	if !uriReg.Match([]byte(d)) {
		return dir, nil
	}

	u, err := url.ParseRequestURI(d)
	if err != nil {
		logrus.Errorf("invalid directory format %v", d)
		return dir, err
	}

	if u.Scheme != "file" {
		logrus.Errorf("only file scheme is supported: %v", d)
		return dir, fmt.Errorf("scheme not suported in directory %v", d)
	}

	dir.Path = filepath.Join(u.Hostname(), u.Path)
	query := u.Query()

	for k, v := range query {
		if len(v) != 1 {
			logrus.Errorf("invalid number of options for %v", v)
			return dir, ErrInvalid
		}

		val := v[0]
		switch strings.ToLower(k) {
		case "format":
			if val != "siva" && val != "git" {
				logrus.Errorf("invalid value in format, it can only "+
					"be siva or git %v", val)
				return dir, ErrInvalid
			}
			dir.Format = val

		case "bare":
			switch val {
			case "true":
				dir.Bare = bareOn
			case "false":
				dir.Bare = bareOff
			case "auto":
				dir.Bare = bareAuto
			default:
				logrus.Errorf("invalid value in bare, it can only "+
					"be true, false, or auto %v", val)
				return dir, ErrInvalid
			}

		case "rooted":
			if val != "true" && val != "false" {
				logrus.Errorf("invalid value in rooted, it can only "+
					"be true or false %v", val)
				return dir, ErrInvalid
			}
			dir.Rooted = (val == "true")

		case "bucket":
			num, err := strconv.Atoi(val)
			if err != nil {
				logrus.Errorf("invalid value in bucket: %v", val)
				return dir, ErrInvalid
			}
			dir.Bucket = num

		default:
			logrus.Errorf("invalid option: %v", k)
			return dir, ErrInvalid
		}
	}

	return dir, nil
}

func discoverBare(d directory) (bool, error) {
	fs := osfs.New(d.Path)

	var bare bool
	if d.Bare == bareAuto {
		b, err := plain.IsFirstRepositoryBare(fs, "/")
		if plain.ErrRepositoriesNotFound.Is(err) {
			logrus.WithField("path", d.Path).
				Errorf("could not find repositories, assuming non bare format")
		} else if err != nil {
			return false, err
		}

		bare = b
	} else {
		bare = d.Bare == bareOn
	}

	return bare, nil
}
