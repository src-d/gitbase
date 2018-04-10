package gitbase

import (
	errors "gopkg.in/src-d/go-errors.v1"
	"gopkg.in/src-d/go-mysql-server.v0/server"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-vitess.v0/mysql"
)

// Session is the custom implementation of a gitbase session.
type Session struct {
	sql.Session
	Pool *RepositoryPool
}

// NewSession creates a new Session.
func NewSession(pool *RepositoryPool) *Session {
	return &Session{
		Session: sql.NewBaseSession(),
		Pool:    pool,
	}
}

// NewSessionBuilder creates a SessionBuilder with the given Repository Pool.
func NewSessionBuilder(pool *RepositoryPool) server.SessionBuilder {
	return func(_ *mysql.Conn) sql.Session {
		return NewSession(pool)
	}
}

// ErrSessionCanceled is returned when session context is canceled
var ErrSessionCanceled = errors.NewKind("session canceled")

// ErrInvalidGitbaseSession is returned when some node expected a GitQuery
// session but received something else.
var ErrInvalidGitbaseSession = errors.NewKind("expecting gitbase session, but received: %T")
