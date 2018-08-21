package gitbase_test

import (
	"context"
	"io/ioutil"
	"testing"

	"github.com/src-d/gitbase"
	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-mysql-server.v0/sql/parse"
	yaml "gopkg.in/yaml.v2"
)

type Query struct {
	ID         string   `yaml:"ID"`
	Name       string   `yaml:"Name,omitempty"`
	Statements []string `yaml:"Statements"`
}

func TestParseRegressionQueries(t *testing.T) {
	require := require.New(t)

	queries, err := loadQueriesYaml("./_testdata/regression.yml")
	require.NoError(err)

	ctx := sql.NewContext(
		context.TODO(),
		sql.WithSession(gitbase.NewSession(gitbase.NewRepositoryPool())),
	)

	for _, q := range queries {
		for _, stmt := range q.Statements {
			if _, err := parse.Parse(ctx, stmt); err != nil {
				require.Failf(err.Error(), "ID: %s, Name: %s, Statement: %s", q.ID, q.Name, stmt)
			}
		}
	}
}

func loadQueriesYaml(file string) ([]Query, error) {
	text, err := ioutil.ReadFile(file)
	if err != nil {
		return nil, err
	}
	var q []Query
	err = yaml.Unmarshal(text, &q)
	if err != nil {
		return nil, err
	}

	return q, nil
}
