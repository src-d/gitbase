package gitbase_test

import (
	"context"
	"io/ioutil"
	"os"
	"testing"

	"github.com/src-d/gitbase"
	"github.com/stretchr/testify/require"
	"github.com/src-d/go-mysql-server/sql"
	"github.com/src-d/go-mysql-server/sql/index/pilosa"
	yaml "gopkg.in/yaml.v2"
)

type Query struct {
	ID         string   `yaml:"ID"`
	Name       string   `yaml:"Name,omitempty"`
	Statements []string `yaml:"Statements"`
}

func TestRegressionQueries(t *testing.T) {
	require := require.New(t)

	engine, pool, cleanup := setup(t)
	defer cleanup()

	tmpDir, err := ioutil.TempDir(os.TempDir(), "pilosa-idx-gitbase")
	require.NoError(err)
	defer os.RemoveAll(tmpDir)
	engine.Catalog.RegisterIndexDriver(pilosa.NewDriver(tmpDir))

	ctx := sql.NewContext(
		context.TODO(),
		sql.WithSession(gitbase.NewSession(pool)),
	)

	queries, err := loadQueriesYaml("./_testdata/regression.yml")
	require.NoError(err)

	for _, q := range queries {
		for _, stmt := range q.Statements {
			_, iter, err := engine.Query(ctx, stmt)
			if err != nil {
				require.Failf(err.Error(), "ID: %s, Name: %s, Statement: %s", q.ID, q.Name, stmt)
			}

			_, err = sql.RowIterToRows(iter)
			if err != nil {
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
