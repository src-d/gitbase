package function

import "gopkg.in/src-d/go-mysql-server.v0/sql"

var functions = map[string]interface{}{
	"is_tag":    NewIsTag,
	"is_remote": NewIsRemote,
}

// Register all the gitquery functions in the SQL catalog.
func Register(c *sql.Catalog) error {
	for k, v := range functions {
		if err := c.RegisterFunction(k, v); err != nil {
			return err
		}
	}

	return nil
}
