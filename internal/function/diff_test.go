package function

import (
	"context"
	"testing"

	"github.com/src-d/gitbase"
	"github.com/stretchr/testify/require"
	fixtures "gopkg.in/src-d/go-git-fixtures.v3"
	"gopkg.in/src-d/go-git.v4/plumbing/cache"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-mysql-server.v0/sql/expression"
)

func TestDiffEval(t *testing.T) {
	require.NoError(t, fixtures.Init())
	defer func() {
		require.NoError(t, fixtures.Clean())
	}()

	path := fixtures.ByTag("worktree").One().Worktree().Root()

	pool := gitbase.NewRepositoryPool(cache.DefaultMaxSize)
	require.NoError(t, pool.AddGitWithID("worktree", path))

	session := gitbase.NewSession(pool)
	ctx := sql.NewContext(context.TODO(), sql.WithSession(session))

	testCases := []struct {
		name     string
		repo     sql.Expression
		from     sql.Expression
		to       sql.Expression
		row      sql.Row
		expected []*diffItem
	}{
		{
			name: "init commit",
			repo: expression.NewGetField(0, sql.Text, "repository_id", false),
			from: nil,
			to:   expression.NewGetField(1, sql.Text, "commit_hash", false),
			row:  sql.NewRow("worktree", "b029517f6300c2da0f4b651b8642506cd6aaf45d"),
			expected: []*diffItem{
				&diffItem{
					Index:  0,
					File:   ".gitignore",
					Chunk:  "*.class\n\n# Mobile Tools for Java (J2ME)\n.mtj.tmp/\n\n# Package Files #\n*.jar\n*.war\n*.ear\n\n# virtual machine crash logs, see http://www.java.com/en/download/help/error_hotspot.xml\nhs_err_pid*\n",
					Status: "+",
				},
				&diffItem{
					Index:  0,
					File:   "LICENSE",
					Chunk:  "The MIT License (MIT)\n\nCopyright (c) 2015 Tyba\n\nPermission is hereby granted, free of charge, to any person obtaining a copy\nof this software and associated documentation files (the \"Software\"), to deal\nin the Software without restriction, including without limitation the rights\nto use, copy, modify, merge, publish, distribute, sublicense, and/or sell\ncopies of the Software, and to permit persons to whom the Software is\nfurnished to do so, subject to the following conditions:\n\nThe above copyright notice and this permission notice shall be included in all\ncopies or substantial portions of the Software.\n\nTHE SOFTWARE IS PROVIDED \"AS IS\", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR\nIMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,\nFITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE\nAUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER\nLIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,\nOUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE\nSOFTWARE.\n\n",
					Status: "+",
				},
			},
		},
		{
			name: "creating changelog",
			repo: expression.NewGetField(0, sql.Text, "repository_id", false),
			from: nil,
			to:   expression.NewGetField(1, sql.Text, "commit_hash", false),
			row:  sql.NewRow("worktree", "b8e471f58bcbca63b07bda20e428190409c2db47"),
			expected: []*diffItem{
				&diffItem{
					Index:  0,
					File:   "CHANGELOG",
					Chunk:  "Initial changelog\n",
					Status: "+",
				},
			},
		},
		{
			name: "creating changelog (with from/to commits)",
			repo: expression.NewGetField(0, sql.Text, "repository_id", false),
			from: expression.NewGetField(1, sql.Text, "from", false),
			to:   expression.NewGetField(2, sql.Text, "to", false),
			row:  sql.NewRow("worktree", "b029517f6300c2da0f4b651b8642506cd6aaf45d", "b8e471f58bcbca63b07bda20e428190409c2db47"),
			expected: []*diffItem{
				&diffItem{
					Index:  0,
					File:   "CHANGELOG",
					Chunk:  "Initial changelog\n",
					Status: "+",
				},
			},
		},
		{
			name: "remove changelog (swap from/to commits)",
			repo: expression.NewGetField(0, sql.Text, "repository_id", false),
			from: expression.NewGetField(1, sql.Text, "from", false),
			to:   expression.NewGetField(2, sql.Text, "to", false),
			row:  sql.NewRow("worktree", "b8e471f58bcbca63b07bda20e428190409c2db47", "b029517f6300c2da0f4b651b8642506cd6aaf45d"),
			expected: []*diffItem{
				&diffItem{
					Index:  0,
					File:   "CHANGELOG",
					Chunk:  "Initial changelog\n",
					Status: "-",
				},
			},
		},
		{
			name:     "binary file",
			repo:     expression.NewGetField(0, sql.Text, "repository_id", false),
			from:     nil,
			to:       expression.NewGetField(1, sql.Text, "to", false),
			row:      sql.NewRow("worktree", "35e85108805c84807bc66a02d91535e1e24b38b9"),
			expected: []*diffItem{},
		},
		{
			name: "HEAD",
			repo: expression.NewGetField(0, sql.Text, "repository_id", false),
			from: nil,
			to:   expression.NewGetField(1, sql.Text, "to", false),
			row:  sql.NewRow("worktree", "HEAD"),
			expected: []*diffItem{
				&diffItem{
					Index:  0,
					File:   "vendor/foo.go",
					Chunk:  "package main\n\nimport \"fmt\"\n\nfunc main() {\n\tfmt.Println(\"Hello, playground\")\n}\n",
					Status: "+",
				},
			},
		},
		{
			name: "HEAD^1",
			repo: expression.NewGetField(0, sql.Text, "repository_id", false),
			from: nil,
			to:   expression.NewGetField(1, sql.Text, "to", false),
			row:  sql.NewRow("worktree", "HEAD", "HEAD^1"),
			expected: []*diffItem{
				&diffItem{
					Index:  0,
					File:   "vendor/foo.go",
					Chunk:  "package main\n\nimport \"fmt\"\n\nfunc main() {\n\tfmt.Println(\"Hello, playground\")\n}\n",
					Status: "+",
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			diff, err := NewDiff(tc.repo, tc.from, tc.to)
			require.NoError(t, err)

			result, err := diff.Eval(ctx, tc.row)
			require.NoError(t, err)

			items, ok := result.([]*diffItem)
			require.True(t, ok)

			require.EqualValues(t, tc.expected, items)
		})
	}
}
