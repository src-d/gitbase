package gitquery

import (
	"context"

	errors "gopkg.in/src-d/go-errors.v1"
	"gopkg.in/src-d/go-mysql-server.v0/server"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-vitess.v0/mysql"
)

// Session is the custom implementation of a gitquery session.
type Session struct {
	sql.Session
	Pool *RepositoryPool
}

// NewSession creates a new Session.
func NewSession(ctx context.Context, pool *RepositoryPool) *Session {
	return &Session{
		Session: sql.NewBaseSession(ctx),
		Pool:    pool,
	}
}

// NewSessionBuilder creates a SessionBuilder with the given Repository Pool.
func NewSessionBuilder(pool *RepositoryPool) server.SessionBuilder {
	return func(ctx context.Context, _ *mysql.Conn) sql.Session {
		return NewSession(ctx, pool)
	}
}

// ErrSessionCanceled is returned when session context is canceled
var ErrSessionCanceled = errors.NewKind("session canceled")

// ErrInvalidGitQuerySession is returned when some node expected a GitQuery
// session but received something else.
var ErrInvalidGitQuerySession = errors.NewKind("expecting gitquery session, but received: %T")
