package e2e

import (
	"database/sql"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
)

var (
	mustRun = flag.Bool("must-run", false, "ensures the tests are run")
	bin     = flag.String("gitbase-bin", "", "path to the gitbase binary to test")
	repos   = flag.String("gitbase-repos", "", "path to the gitbase repos to test")
	version = flag.String("gitbase-version", "", "(optional) version of the binary")
)

func TestMain(m *testing.M) {
	flag.Parse()

	if *repos == "" {
		fmt.Println("gitbase-repos not provided")
		if *mustRun {
			os.Exit(1)
		} else {
			os.Exit(0)
		}
	}

	path, err := exec.LookPath(*bin)
	if err != nil {
		fmt.Println("gitbase-bin not provided")
		if *mustRun {
			os.Exit(1)
		} else {
			os.Exit(0)
		}
	}

	var done = make(chan error)
	cmd := exec.Command(
		path,
		"server",
		"--directories="+*repos,
		"--host=127.0.0.1",
		"--port=3308",
		"--index=indexes",
	)

	if err := cmd.Start(); err != nil {
		fmt.Println("unable to start gitbase binary:", err)
		os.Exit(1)
	}

	go func() {
		switch err := cmd.Wait().(type) {
		case *exec.ExitError:
			done <- nil
		default:
			done <- err
		}
	}()

	time.Sleep(500 * time.Millisecond)

	code := m.Run()

	if err := cmd.Process.Signal(os.Interrupt); err != nil {
		fmt.Println("problem stopping binary:", err)
		os.Exit(1)
	}

	if err := <-done; err != nil {
		fmt.Println("problem executing binary:", err)
		os.Exit(1)
	}

	os.Exit(code)
}

func connect(t *testing.T) (*sql.DB, func()) {
	db, err := sql.Open("mysql", "root:@tcp(127.0.0.1:3308)/gitbase")
	if err != nil {
		t.Errorf("unexpected error connecting to gitbase: %s", err)
	}

	return db, func() {
		require.NoError(t, db.Close())
	}
}

func TestVersion(t *testing.T) {
	if *version == "" {
		t.Skip("no version provided, skipping")
	}
	db, cleanup := connect(t)
	defer cleanup()

	require := require.New(t)

	var v string
	require.NoError(db.QueryRow("SELECT VERSION()").Scan(&v))
	require.Equal(fmt.Sprintf("8.0.11-%s", *version), v)
}
