package function

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"strconv"

	lru "github.com/hashicorp/golang-lru"
	enry "gopkg.in/src-d/enry.v1"
	"github.com/src-d/go-mysql-server/sql"
)

const (
	languageCacheSizeKey     = "GITBASE_LANGUAGE_CACHE_SIZE"
	defaultLanguageCacheSize = 10000
)

func languageCacheSize() int {
	v := os.Getenv(languageCacheSizeKey)
	size, err := strconv.Atoi(v)
	if err != nil || size <= 0 {
		size = defaultLanguageCacheSize
	}

	return size
}

var languageCache *lru.TwoQueueCache

func init() {
	var err error
	languageCache, err = lru.New2Q(languageCacheSize())
	if err != nil {
		panic(fmt.Errorf("cannot initialize language cache: %s", err))
	}
}

// Language gets the language of a file given its path and
// the optional content of the file.
type Language struct {
	Left  sql.Expression
	Right sql.Expression
}

// NewLanguage creates a new Language UDF.
func NewLanguage(args ...sql.Expression) (sql.Expression, error) {
	var left, right sql.Expression
	switch len(args) {
	case 1:
		left = args[0]
	case 2:
		left = args[0]
		right = args[1]
	default:
		return nil, sql.ErrInvalidArgumentNumber.New("1 or 2", len(args))
	}

	return &Language{left, right}, nil
}

// Resolved implements the Expression interface.
func (f *Language) Resolved() bool {
	return f.Left.Resolved() && (f.Right == nil || f.Right.Resolved())
}

func (f *Language) String() string {
	if f.Right == nil {
		return fmt.Sprintf("language(%s)", f.Left)
	}
	return fmt.Sprintf("language(%s, %s)", f.Left, f.Right)
}

// IsNullable implements the Expression interface.
func (f *Language) IsNullable() bool {
	return f.Left.IsNullable() || (f.Right != nil && f.Right.IsNullable())
}

// Type implements the Expression interface.
func (Language) Type() sql.Type {
	return sql.Text
}

// TransformUp implements the Expression interface.
func (f *Language) TransformUp(fn sql.TransformExprFunc) (sql.Expression, error) {
	left, err := f.Left.TransformUp(fn)
	if err != nil {
		return nil, err
	}

	var right sql.Expression
	if f.Right != nil {
		right, err = f.Right.TransformUp(fn)
		if err != nil {
			return nil, err
		}
	}

	return fn(&Language{left, right})
}

// Eval implements the Expression interface.
func (f *Language) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	span, ctx := ctx.Span("gitbase.Language")
	defer span.Finish()

	left, err := f.Left.Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	if left == nil {
		return nil, nil
	}

	left, err = sql.Text.Convert(left)
	if err != nil {
		return nil, err
	}

	path := left.(string)
	var blob []byte

	if f.Right != nil {
		right, err := f.Right.Eval(ctx, row)
		if err != nil {
			return nil, err
		}

		if right == nil {
			return nil, nil
		}

		right, err = sql.Blob.Convert(right)
		if err != nil {
			return nil, err
		}

		blob = right.([]byte)
	}

	var hash [8]byte
	if len(blob) > 0 {
		hash = languageHash(path, blob)
		value, ok := languageCache.Get(hash)
		if ok {
			return value, nil
		}
	}

	lang := enry.GetLanguage(path, blob)
	if lang == "" {
		return nil, nil
	}

	if len(blob) > 0 {
		languageCache.Add(hash, lang)
	}

	return lang, nil
}

func languageHash(filename string, blob []byte) [8]byte {
	fh := filenameHash(filename)
	bh := blobHash(blob)

	var result [8]byte
	copy(result[:], fh)
	copy(result[4:], bh)
	return result
}

func blobHash(blob []byte) []byte {
	if len(blob) == 0 {
		return nil
	}

	n := crc32.ChecksumIEEE(blob)
	hash := make([]byte, 4)
	binary.LittleEndian.PutUint32(hash, n)
	return hash
}

func filenameHash(filename string) []byte {
	n := crc32.ChecksumIEEE([]byte(filename))
	hash := make([]byte, 4)
	binary.LittleEndian.PutUint32(hash, n)
	return hash
}

// Children implements the Expression interface.
func (f *Language) Children() []sql.Expression {
	if f.Right == nil {
		return []sql.Expression{f.Left}
	}

	return []sql.Expression{f.Left, f.Right}
}
