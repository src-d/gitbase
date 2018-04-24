package cmd

import (
	"net"
	"strconv"

	"github.com/src-d/gitbase"
	"github.com/src-d/gitbase/internal/function"
	"github.com/src-d/gitbase/internal/rule"

	"github.com/sirupsen/logrus"
	sqle "gopkg.in/src-d/go-mysql-server.v0"
	"gopkg.in/src-d/go-mysql-server.v0/server"
	"gopkg.in/src-d/go-vitess.v0/mysql"
)

const (
	ServerDescription = "Starts a gitbase server instance"
	ServerHelp        = ServerDescription + "\n\n" +
		"The squashing tables and pushing down join conditions is still a\n" +
		"work in progress and unstable,disable by default can be enabled\n" +
		"using a not empty value at UNSTABLE_SQUASH_ENABLE env variable.\n\n" +
		"By default when gitbase encounters and error in a repository it\n" +
		"stops the query. With GITBASE_SKIP_GIT_ERRORS variable it won't\n" +
		"complain and just skip those rows or repositories."
)

// Server represents the `server` command of gitbase cli tool.
type Server struct {
	Verbose  bool     `short:"v" description:"Activates the verbose mode"`
	Git      []string `short:"g" long:"git" description:"Path where the git repositories are located, multiple directories can be defined"`
	Siva     []string `long:"siva" description:"Path where the siva repositories are located, multiple directories can be defined"`
	Host     string   `short:"h" long:"host" default:"localhost" description:"Host where the server is going to listen"`
	Port     int      `short:"p" long:"port" default:"3306" description:"Port where the server is going to listen"`
	User     string   `short:"u" long:"user" default:"root" description:"User name used for connection"`
	Password string   `short:"P" long:"password" default:"" description:"Password used for connection"`

	// UnstableSquash quashing tables and pushing down join conditions is still
	// a work in progress and unstable. To enable it, the UNSTABLE_SQUASH_ENABLE
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

	logrus.Info("starting server")
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

	if c.UnstableSquash {
		logrus.Warn("unstable squash tables rule is enabled")
		c.engine.Analyzer.AddRule(rule.SquashJoinsRule, rule.SquashJoins)
	}

	return nil
}

func (c *Server) addDirectories() error {
	if len(c.Git) == 0 && len(c.Siva) == 0 {
		logrus.Error("At least one git folder or siva folder should be provided.")
	}

	for _, dir := range c.Git {
		if err := c.addGitDirectory(dir); err != nil {
			return err
		}
	}

	for _, dir := range c.Siva {
		if err := c.addSivaDirectory(dir); err != nil {
			return err
		}
	}

	return nil
}

func (c *Server) addGitDirectory(folder string) error {
	logrus.WithField("dir", c.Git).Debug("git repositories directory added")
	return c.pool.AddDir(folder)
}

func (c *Server) addSivaDirectory(folder string) error {
	logrus.WithField("dir", c.Git).Debug("siva repositories directory added")
	return c.pool.AddSivaDir(folder)
}
