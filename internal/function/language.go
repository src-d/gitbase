package function

import (
	"fmt"
	"hash/crc32"
	"os"
	"strconv"
	"sync"

	enry "github.com/src-d/enry/v2"
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

var (
	languageMut   sync.Mutex
	languageCache sql.KeyValueCache
)

func getLanguageCache(ctx *sql.Context) sql.KeyValueCache {
	languageMut.Lock()
	defer languageMut.Unlock()
	if languageCache == nil {
		// Dispose function is ignored because the cache will never be disposed
		// until the program dies.
		languageCache, _ = ctx.Memory.NewLRUCache(uint(languageCacheSize()))
	}

	return languageCache
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

// WithChildren implements the Expression interface.
func (f *Language) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	expected := 1
	if f.Right != nil {
		expected = 2
	}

	if len(children) != expected {
		return nil, sql.ErrInvalidChildrenNumber.New(f, len(children), expected)
	}

	return NewLanguage(children...)
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

	languageCache := getLanguageCache(ctx)

	var hash uint64
	if len(blob) > 0 {
		hash = languageHash(path, blob)
		value, err := languageCache.Get(hash)
		if err == nil {
			return value, nil
		}
	}

	lang := enry.GetLanguage(path, blob)
	if lang == "" {
		return nil, nil
	}

	if len(blob) > 0 {
		if err := languageCache.Put(hash, lang); err != nil {
			return nil, err
		}
	}

	return lang, nil
}

func languageHash(filename string, blob []byte) uint64 {
	fh := filenameHash(filename)
	bh := blobHash(blob)

	return uint64(fh)<<32 | uint64(bh)
}

func blobHash(blob []byte) uint32 {
	if len(blob) == 0 {
		return 0
	}

	return crc32.ChecksumIEEE(blob)
}

func filenameHash(filename string) uint32 {
	return crc32.ChecksumIEEE([]byte(filename))
}

// Children implements the Expression interface.
func (f *Language) Children() []sql.Expression {
	if f.Right == nil {
		return []sql.Expression{f.Left}
	}

	return []sql.Expression{f.Left, f.Right}
}
