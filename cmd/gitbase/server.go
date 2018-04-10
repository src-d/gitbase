package main

import (
	"net"
	"os"
	"strconv"

	"github.com/src-d/gitbase"
	"github.com/src-d/gitbase/internal/function"
	"github.com/src-d/gitbase/internal/rule"

	sqle "gopkg.in/src-d/go-mysql-server.v0"
	"gopkg.in/src-d/go-mysql-server.v0/server"
	"gopkg.in/src-d/go-vitess.v0/mysql"
)

// Squashing tables and pushing down join conditions is still a work in
// progress and unstable. To enable it, the UNSTABLE_SQUASH_ENABLE must
// not be empty.
var enableUnstableSquash = os.Getenv("UNSTABLE_SQUASH_ENABLE") != ""

// CmdServer defines server command
type CmdServer struct {
	cmd

	Git      string `short:"g" long:"git" description:"Path where the git repositories are located, one per dir"`
	Host     string `short:"h" long:"host" default:"localhost" description:"Host where the server is going to listen"`
	Port     int    `short:"p" long:"port" default:"3306" description:"Port where the server is going to listen"`
	User     string `short:"u" long:"user" default:"root" description:"User name used for connection"`
	Password string `short:"P" long:"password" default:"" description:"Password used for connection"`

	engine *sqle.Engine
	pool   *gitbase.RepositoryPool
	name   string
}

func (c *CmdServer) buildDatabase() error {
	if c.engine == nil {
		c.engine = sqle.New()
	}

	c.print("opening %q repository...\n", c.Git)

	var err error

	pool := gitbase.NewRepositoryPool()
	c.pool = &pool
	err = c.pool.AddDir(c.Git)
	if err != nil {
		return err
	}

	c.engine.AddDatabase(gitbase.NewDatabase(c.name))
	c.engine.Catalog.RegisterFunctions(function.Functions)

	if enableUnstableSquash {
		c.engine.Analyzer.AddRule(rule.SquashJoinsRule, rule.SquashJoins)
	}

	return nil
}

// Execute starts the server
func (c *CmdServer) Execute(args []string) error {
	if err := c.buildDatabase(); err != nil {
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
		gitbase.NewSessionBuilder(c.pool),
	)
	if err != nil {
		return err
	}

	return s.Start()
}
