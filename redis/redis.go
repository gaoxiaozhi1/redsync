package redis

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"io"
	"time"
)

// Pool maintains a pool of Redis connections.
type Pool interface {
	Get(ctx context.Context) (Conn, error)
}

// Conn is a single Redis connection.
// Conn是一个单一的Redis连接。
type Conn interface {
	Get(name string) (string, error)
	Set(name string, value string) (bool, error)
	// SetNX 新增键值对，并设置过期时间
	SetNX(name string, value string, expiry time.Duration) (bool, error)
	Eval(script *Script, keysAndArgs ...interface{}) (interface{}, error)
	PTTL(name string) (time.Duration, error)
	Close() error
}

// Script encapsulates the source, hash and key count for a Lua script.
// 脚本封装Lua脚本的源代码、哈希和密钥计数。
// Taken from https://github.com/gomodule/redigo/blob/46992b0f02f74066bcdfd9b03e33bc03abd10dc7/redis/script.go#L24-L30
type Script struct {
	KeyCount int
	Src      string
	Hash     string
}

// NewScript returns a new script object. If keyCount is greater than or equal
// to zero, then the count is automatically inserted in the EVAL command
// argument list. If keyCount is less than zero, then the application supplies
// the count as the first value in the keysAndArgs argument to the Do, Send and
// SendHash methods.
// NewScript 返回一个新的脚本对象。
// 如果 keyCount大于或等于0，则keyCount将自动插入 EVAL命令的参数列表中。
// 如果keyCount小于0则应用程序将 keyCount作为 Do、Send和 SendHash 方法的keysAndArgs 参数中的第一个值提供。
// Taken from https://github.com/gomodule/redigo/blob/46992b0f02f74066bcdfd9b03e33bc03abd10dc7/redis/script.go#L32-L41
func NewScript(keyCount int, src string) *Script {
	h := sha1.New()
	_, _ = io.WriteString(h, src)
	return &Script{keyCount, src, hex.EncodeToString(h.Sum(nil))}
}
