package e2e

import (
	"database/sql"
	"flag"
	"fmt"
	"net"
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

	port int
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
		fmt.Println("gitbase-bin not provided:", err)
		if *mustRun {
			os.Exit(1)
		} else {
			os.Exit(0)
		}
	}

	port, err = findPort()
	if err != nil {
		fmt.Println("unable to find an available port: ", err)
		os.Exit(1)
	}

	var done = make(chan error)
	cmd := exec.Command(
		path,
		"server",
		"--directories="+*repos,
		"--host=127.0.0.1",
		fmt.Sprintf("--port=%d", port),
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
	db, err := sql.Open("mysql", fmt.Sprintf("root:@tcp(127.0.0.1:%d)/gitbase", port))
	if err != nil {
		t.Errorf("unexpected error connecting to gitbase: %s", err)
	}

	return db, func() {
		require.NoError(t, db.Close())
	}
}

func findPort() (int, error) {
	addr, err := net.ResolveTCPAddr("tcp", "localhost:0")
	if err != nil {
		return 0, err
	}

	l, err := net.ListenTCP("tcp", addr)
	if err != nil {
		return 0, err
	}

	port := l.Addr().(*net.TCPAddr).Port
	_ = l.Close()

	return port, nil
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
