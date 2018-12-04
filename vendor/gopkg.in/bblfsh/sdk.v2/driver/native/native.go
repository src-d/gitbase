package native

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"sync"
	"time"

	"github.com/opentracing/opentracing-go"

	"gopkg.in/bblfsh/sdk.v2/driver"
	derrors "gopkg.in/bblfsh/sdk.v2/driver/errors"
	"gopkg.in/bblfsh/sdk.v2/driver/native/jsonlines"
	"gopkg.in/bblfsh/sdk.v2/uast/nodes"
	serrors "gopkg.in/src-d/go-errors.v1"
)

var (
	// Binary default location of the native driver binary. Should not
	// override this variable unless you know what are you doing.
	Binary = "/opt/driver/bin/native"
)

const (
	closeTimeout = time.Second * 5
)

var (
	ErrNotRunning = serrors.NewKind("native driver is not running")
)

func NewDriver(enc Encoding) driver.Native {
	return NewDriverAt("", enc)
}

func NewDriverAt(bin string, enc Encoding) driver.Native {
	if bin == "" {
		bin = Binary
	}
	if enc == "" {
		enc = UTF8
	}
	return &Driver{bin: bin, ec: enc}
}

// Driver is a wrapper of the native command. The operations with the
// driver are synchronous by design, this is controlled by a mutex. This means
// that only one parse request can attend at the same time.
type Driver struct {
	bin     string
	ec      Encoding
	running bool

	mu     sync.Mutex
	enc    jsonlines.Encoder
	dec    jsonlines.Decoder
	stdin  *os.File
	stdout *os.File
	cmd    *exec.Cmd
}

// Start executes the given native driver and prepares it to parse code.
func (d *Driver) Start() error {
	d.cmd = exec.Command(d.bin)
	d.cmd.Stderr = os.Stderr

	var (
		err           error
		stdin, stdout *os.File
	)

	stdin, d.stdin, err = os.Pipe()
	if err != nil {
		return err
	}

	d.stdout, stdout, err = os.Pipe()
	if err != nil {
		stdin.Close()
		d.stdin.Close()
		return err
	}
	d.cmd.Stdin = stdin
	d.cmd.Stdout = stdout

	d.enc = jsonlines.NewEncoder(d.stdin)
	d.dec = jsonlines.NewDecoder(d.stdout)

	err = d.cmd.Start()
	if err == nil {
		d.running = true
		return nil
	}
	d.stdin.Close()
	d.stdout.Close()
	stdin.Close()
	stdout.Close()
	return err
}

// parseRequest is the request used to communicate the driver with the
// native driver via json.
type parseRequest struct {
	Content  string   `json:"content"`
	Encoding Encoding `json:"Encoding"`
}

var _ json.Unmarshaler = (*parseResponse)(nil)

// parseResponse is the reply to parseRequest by the native parser.
type parseResponse struct {
	Status status     `json:"status"`
	Errors []string   `json:"errors"`
	AST    nodes.Node `json:"ast"`
}

func (r *parseResponse) UnmarshalJSON(data []byte) error {
	var resp struct {
		Status status      `json:"status"`
		Errors []string    `json:"errors"`
		AST    interface{} `json:"ast"`
	}
	if err := json.Unmarshal(data, &resp); err != nil {
		return err
	}
	ast, err := nodes.ToNode(resp.AST, nil)
	if err != nil {
		return err
	}
	*r = parseResponse{
		Status: resp.Status,
		Errors: resp.Errors,
		AST:    ast,
	}
	return nil
}

func (d *Driver) writeRequest(ctx context.Context, req *parseRequest) error {
	deadline, _ := ctx.Deadline()

	sp, _ := opentracing.StartSpanFromContext(ctx, "bblfsh.native.Parse.encodeReq")
	defer sp.Finish()

	if !deadline.IsZero() {
		d.stdin.SetWriteDeadline(deadline)
		defer d.stdin.SetWriteDeadline(time.Time{})
	}

	return d.enc.Encode(req)
}

func (d *Driver) readResponse(ctx context.Context) (*parseResponse, error) {
	deadline, _ := ctx.Deadline()

	sp, _ := opentracing.StartSpanFromContext(ctx, "bblfsh.native.Parse.decodeResp")
	defer sp.Finish()

	if !deadline.IsZero() {
		d.stdout.SetReadDeadline(deadline)
		defer d.stdout.SetReadDeadline(time.Time{})
	}

	var r parseResponse
	err := d.dec.Decode(&r)
	if err != nil {
		return nil, err
	}
	return &r, nil
}

// Parse sends a request to the native driver and returns its response.
func (d *Driver) Parse(rctx context.Context, src string) (nodes.Node, error) {
	sp, ctx := opentracing.StartSpanFromContext(rctx, "bblfsh.native.Parse")
	defer sp.Finish()

	if !d.running {
		return nil, driver.ErrDriverFailure.Wrap(ErrNotRunning.New())
	}

	str, err := d.ec.Encode(src)
	if err != nil {
		return nil, driver.ErrDriverFailure.Wrap(err)
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	err = d.writeRequest(ctx, &parseRequest{
		Content: str, Encoding: d.ec,
	})
	if err != nil {
		// Cannot write data - this means the stream is broken or driver crashed.
		// We will try to recover by reading the response, but since it might be
		// a stack trace or an error message, we will read it as a "raw" value.
		// This preserves an original text instead of failing with decoding error.
		var raw json.RawMessage
		// TODO: this reads a single line only; we can be smarter and read the whole log if driver cannot recover
		if err := d.dec.Decode(&raw); err != nil {
			// stream is broken on both sides, cannot get additional info
			return nil, driver.ErrDriverFailure.Wrap(err)
		}
		return nil, driver.ErrDriverFailure.Wrap(fmt.Errorf("error: %v; %s", err, string(raw)))
	}

	r, err := d.readResponse(ctx)
	if err != nil {
		return nil, driver.ErrDriverFailure.Wrap(err)
	}
	if r.Status == statusOK {
		return r.AST, nil
	}
	errs := make([]error, 0, len(r.Errors))
	for _, s := range r.Errors {
		errs = append(errs, errors.New(s))
	}
	err = derrors.Join(errs)
	switch r.Status {
	case statusError:
		// parsing error, wrapping will be done on a higher level
	case statusFatal:
		err = driver.ErrDriverFailure.Wrap(err)
		r.AST = nil // do not allow to propagate AST with Fatal error
	default:
		return nil, fmt.Errorf("unsupported status: %v", r.Status)
	}
	return r.AST, err
}

// Stop stops the execution of the native driver.
func (d *Driver) Close() error {
	var last error
	if err := d.stdin.Close(); err != nil {
		last = err
	}
	errc := make(chan error, 1)
	go func() {
		errc <- d.cmd.Wait()
	}()
	timeout := time.NewTimer(closeTimeout)
	select {
	case err := <-errc:
		timeout.Stop()
		if err != nil {
			last = err
		}
	case <-timeout.C:
		d.cmd.Process.Kill()
	}
	err2 := d.stdout.Close()
	if last != nil {
		return last
	}
	if er, ok := err2.(*os.PathError); ok && er.Err == os.ErrClosed {
		err2 = nil
	}
	if err2 != nil {
		last = err2
	}
	return last
}

var _ json.Unmarshaler = (*status)(nil)

type status string

func (s *status) UnmarshalJSON(data []byte) error {
	var str string
	if err := json.Unmarshal(data, &str); err != nil {
		return err
	}
	str = strings.ToLower(str)
	*s = status(str)
	return nil
}

const (
	statusOK = status("ok")
	// statusError is replied when the driver has got the AST with errors.
	statusError = status("error")
	// statusFatal is replied when the driver hasn't could get the AST.
	statusFatal = status("fatal")
)

var _ json.Unmarshaler = (*Encoding)(nil)

// Encoding is the Encoding used for the content string. Currently only
// UTF-8 or Base64 encodings are supported. You should use UTF-8 if you can
// and Base64 as a fallback.
type Encoding string

const (
	UTF8   = Encoding("utf8")
	Base64 = Encoding("base64")
)

func (e *Encoding) UnmarshalJSON(data []byte) error {
	var str string
	if err := json.Unmarshal(data, &str); err != nil {
		return err
	}
	str = strings.ToLower(str)
	*e = Encoding(str)
	return nil
}

// Encode converts UTF8 string into specified Encoding.
func (e Encoding) Encode(s string) (string, error) {
	switch e {
	case UTF8:
		return s, nil
	case Base64:
		s = base64.StdEncoding.EncodeToString([]byte(s))
		return s, nil
	default:
		return "", fmt.Errorf("invalid Encoding: %v", e)
	}
}

// Decode converts specified Encoding into UTF8.
func (e Encoding) Decode(s string) (string, error) {
	switch e {
	case UTF8:
		return s, nil
	case Base64:
		b, err := base64.StdEncoding.DecodeString(s)
		if err != nil {
			return "", err
		}
		return string(b), nil
	default:
		return "", fmt.Errorf("invalid Encoding: %v", e)
	}
}
