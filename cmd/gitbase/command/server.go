package command

import (
	"net"
	"os"
	"path/filepath"
	"strconv"

	"github.com/src-d/gitbase"
	"github.com/src-d/gitbase/internal/function"
	"github.com/src-d/gitbase/internal/rule"

	gopilosa "github.com/pilosa/go-pilosa"
	"github.com/sirupsen/logrus"
	sqle "gopkg.in/src-d/go-mysql-server.v0"
	"gopkg.in/src-d/go-mysql-server.v0/server"
	"gopkg.in/src-d/go-mysql-server.v0/sql/index/pilosa"
	"gopkg.in/src-d/go-vitess.v0/mysql"
)

const (
	ServerDescription = "Starts a gitbase server instance"
	ServerHelp        = ServerDescription + "\n\n" +
		"The squashing tables and pushing down join conditions is still a\n" +
		"work in progress and unstable,disable by default can be enabled\n" +
		"using a not empty value at GITBASE_UNSTABLE_SQUASH_ENABLE env variable.\n\n" +
		"By default when gitbase encounters and error in a repository it\n" +
		"stops the query. With GITBASE_SKIP_GIT_ERRORS variable it won't\n" +
		"complain and just skip those rows or repositories."
)

// Server represents the `server` command of gitbase cli tool.
type Server struct {
	Verbose   bool     `short:"v" description:"Activates the verbose mode"`
	Git       []string `short:"g" long:"git" description:"Path where the git repositories are located, multiple directories can be defined. Accepts globs."`
	Siva      []string `long:"siva" description:"Path where the siva repositories are located, multiple directories can be defined. Accepts globs."`
	Host      string   `short:"h" long:"host" default:"localhost" description:"Host where the server is going to listen"`
	Port      int      `short:"p" long:"port" default:"3306" description:"Port where the server is going to listen"`
	User      string   `short:"u" long:"user" default:"root" description:"User name used for connection"`
	Password  string   `short:"P" long:"password" default:"" description:"Password used for connection"`
	PilosaURL string   `long:"pilosa" default:"http://localhost:10101" description:"URL to your pilosa server"`
	IndexDir  string   `short:"i" long:"index" default:"/var/lib/gitbase/index" description:"Directory where the gitbase indexes information will be persisted."`

	// UnstableSquash quashing tables and pushing down join conditions is still
	// a work in progress and unstable. To enable it, the GITBASE_UNSTABLE_SQUASH_ENABLE
	// must not be empty.
	UnstableSquash bool
	// IgnoreGitErrors by default when gitbase encounters and error in a
	// repository it stops the query. With this parameter it won't complain and
	// just skip those rows or repositories.
	SkipGitErrors bool

	engine *sqle.Engine
	pool   *gitbase.RepositoryPool
	name   string
}

// Execute starts a new gitbase server based on provided configuration, it
// honors the go-flags.Commander interface.
func (c *Server) Execute(args []string) error {
	if c.Verbose {
		logrus.SetLevel(logrus.DebugLevel)
	}

	if err := c.buildDatabase(); err != nil {
		logrus.WithField("error", err).Fatal("unable to start database server")
		return err
	}

	auth := mysql.NewAuthServerStatic()
	auth.Entries[c.User] = []*mysql.AuthServerStaticEntry{
		{Password: c.Password},
	}

	hostString := net.JoinHostPort(c.Host, strconv.Itoa(c.Port))
	s, err := server.NewServer(
		server.Config{
			Protocol: "tcp",
			Address:  hostString,
			Auth:     auth,
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
		c.engine = sqle.New()
	}

	c.pool = gitbase.NewRepositoryPool()

	if err := c.addDirectories(); err != nil {
		return err
	}

	c.engine.AddDatabase(gitbase.NewDatabase(c.name))
	logrus.WithField("db", c.name).Debug("registered database to catalog")

	c.engine.Catalog.RegisterFunctions(function.Functions)
	logrus.Debug("registered all available functions in catalog")

	if err := c.registerDrivers(); err != nil {
		return err
	}

	if c.UnstableSquash {
		logrus.Warn("unstable squash tables rule is enabled")
		c.engine.Analyzer.AddRule(rule.SquashJoinsRule, rule.SquashJoins)
	}

	return nil
}

func (c *Server) registerDrivers() error {
	if err := os.MkdirAll(c.IndexDir, 0755); err != nil {
		return err
	}

	logrus.Debug("created index storage")

	client, err := gopilosa.NewClient(c.PilosaURL)
	if err != nil {
		return err
	}

	logrus.Debug("established connection with pilosa")

	c.engine.Catalog.RegisterIndexDriver(pilosa.NewDriver(c.IndexDir, client))
	logrus.Debug("registered pilosa index driver")

	return nil
}

func (c *Server) addDirectories() error {
	if len(c.Git) == 0 && len(c.Siva) == 0 {
		logrus.Error("At least one git folder or siva folder should be provided.")
	}

	for _, pattern := range c.Git {
		if err := c.addGitPattern(pattern); err != nil {
			return err
		}
	}

	for _, pattern := range c.Siva {
		if err := c.addSivaPattern(pattern); err != nil {
			return err
		}
	}

	return nil
}

func (c *Server) addGitPattern(pattern string) error {
	matches, err := filepath.Glob(pattern)
	if err != nil {
		return err
	}

	for _, m := range matches {
		logrus.WithField("dir", m).Debug("git repositories directory added")
		if err := c.pool.AddDir(m); err != nil {
			return err
		}
	}

	return nil
}

func (c *Server) addSivaPattern(pattern string) error {
	matches, err := filepath.Glob(pattern)
	if err != nil {
		return err
	}

	for _, m := range matches {
		logrus.WithField("dir", m).Debug("siva repositories directory added")
		if err := c.pool.AddSivaDir(m); err != nil {
			return err
		}
	}

	return nil
}
