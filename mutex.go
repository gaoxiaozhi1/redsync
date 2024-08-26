package redsync

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"time"

	"github.com/go-redsync/redsync/v4/redis"
	"github.com/hashicorp/go-multierror"
)

// A DelayFunc is used to decide the amount of time to wait between retries.
// 决定重试之间的等待时间。
type DelayFunc func(tries int) time.Duration

// A Mutex is a distributed mutual exclusion lock.
type Mutex struct {
	name   string        // 锁的键，在 Redis 中表示这个锁。
	expiry time.Duration // 锁的过期时间，锁在这个时间段后自动释放。

	tries int // 尝试获取锁的重试次数，如果初次获取锁失败，会根据 tries 的值进行多次重试。
	// delayFunc 一个函数类型，用于决定重试之间的等待时间。每次重试前会调用这个函数以获取等待时间。
	delayFunc DelayFunc // 决定重试之间的等待时间

	driftFactor   float64 // 漂移因子，用于计算锁的有效期。这是为了补偿可能的时钟漂移。
	timeoutFactor float64 // 超时因子，用于计算锁的超时时间。

	quorum int // 多节点分布式锁的法定人数，通常是集群中需要同意锁操作的节点数量。

	genValueFunc  func() (string, error) // 一个生成锁值的函数，用于生成唯一的锁值。
	value         string                 // 锁的值，通常是由 genValueFunc 生成的，确保锁的唯一性。
	until         time.Time              // 锁的有效期，表示锁在该时间之前是有效的。
	shuffle       bool                   // 是否打乱节点顺序，在分布式锁实现中可以用来避免多个客户端同时访问相同的节点顺序，减少冲突。
	failFast      bool                   // 是否快速失败，如果设置为 true，在某些条件下会立即返回失败，而不是进行重试。
	setNXOnExtend bool                   // 扩展锁时是否使用 SET NX 选项。SET NX 表示只有在键不存在时才设置键，可以用于扩展锁的有效期。

	pools []redis.Pool // Redis 连接池的数组，表示多个 Redis 连接池，用于分布式锁的多节点操作。
}

// Name returns mutex name (i.e. the Redis key).
// 返回key
func (m *Mutex) Name() string {
	return m.name
}

// Value returns the current random value. The value will be empty until a lock is acquired (or WithValue option is used).
// 返回当前随机值。在获取锁之前，该值将为空(或使用 WithValue 选项)
func (m *Mutex) Value() string {
	return m.value
}

// Until returns the time of validity of acquired lock. The value will be zero value until a lock is acquired.
// 获取的锁的有效时间。在获取锁之前，该值将为零值。
func (m *Mutex) Until() time.Time {
	return m.until
}

// TryLock only attempts to lock once and returns immediately regardless of success or failure without retrying.
// 尝试锁定一次，不论成功与否，立即返回，无需重试。
func (m *Mutex) TryLock() error {
	return m.TryLockContext(context.Background())
}

// TryLockContext only attempts to lock once and returns immediately regardless of success or failure without retrying.
// 尝试锁定一次，不论成功与否，立即返回，无需重试。
func (m *Mutex) TryLockContext(ctx context.Context) error {
	return m.lockContext(ctx, 1)
}

// Lock locks m. In case it returns an error on failure, you may retry to acquire the lock by calling this method again.
func (m *Mutex) Lock() error {
	return m.LockContext(context.Background())
}

// LockContext locks m. In case it returns an error on failure, you may retry to acquire the lock by calling this method again.
func (m *Mutex) LockContext(ctx context.Context) error {
	return m.lockContext(ctx, m.tries)
}

// lockContext locks m. In case it returns an error on failure, you may retry to acquire the lock by calling this method again.
func (m *Mutex) lockContext(ctx context.Context, tries int) error {
	if ctx == nil {
		ctx = context.Background()
	}
	// 一个生成锁值的函数，用于生成唯一的锁值。
	value, err := m.genValueFunc()
	if err != nil {
		return err
	}

	var timer *time.Timer
	// 尝试tries获取锁
	for i := 0; i < tries; i++ {
		if i != 0 {
			// 可以自定义调整 这次重新获取锁的等待时间
			if timer == nil {
				timer = time.NewTimer(m.delayFunc(i))
			} else {
				timer.Reset(m.delayFunc(i))
			}

			select {
			case <-ctx.Done():
				timer.Stop()
				// Exit early if the context is done.
				// 如果上下文完成，请提前退出。
				return ErrFailed
			case <-timer.C: // 到达等待的时间，就可以进行后续重新获取锁的重试操作啦
				// Fall-through when the delay timer completes.
				// 延迟计时器完成时跳转。
			}
		}

		start := time.Now()

		// 这段代码定义并立即执行了一个匿名函数，并将其返回值赋给变量 n 和 err。
		n, err := func() (int, error) {
			// 创建一个带超时的上下文，超时时间为 m.expiry * m.timeoutFactor
			ctx, cancel := context.WithTimeout(ctx, time.Duration(int64(float64(m.expiry)*m.timeoutFactor)))
			// defer cancel()：确保在函数返回时调用 cancel 函数，以释放与上下文关联的资源，防止资源泄露。
			defer cancel()

			// 异步地在连接池上执行获取锁的操作
			// m.actOnPoolsAsync：这是一个自定义方法，它接受一个函数作为参数并在 Redis 连接池上异步执行该函数。
			return m.actOnPoolsAsync(
				// 内部传入的函数 func(pool redis.Pool) (bool, error) 定义了在每个连接池上执行的具体操作。
				func(pool redis.Pool) (bool, error) {
					// 在连接池上新增键值对，并设置过期时间
					return m.acquire(ctx, pool, value)
				},
			)
		}()

		now := time.Now()
		until := now.Add(m.expiry - now.Sub(start) - time.Duration(int64(float64(m.expiry)*m.driftFactor)))
		if n >= m.quorum && now.Before(until) {
			m.value = value
			m.until = until
			return nil
		}
		_, _ = func() (int, error) {
			ctx, cancel := context.WithTimeout(ctx, time.Duration(int64(float64(m.expiry)*m.timeoutFactor)))
			defer cancel()
			return m.actOnPoolsAsync(func(pool redis.Pool) (bool, error) {
				return m.release(ctx, pool, value)
			})
		}()
		if i == tries-1 && err != nil {
			return err
		}
	}

	return ErrFailed
}

// Unlock unlocks m and returns the status of unlock.
func (m *Mutex) Unlock() (bool, error) {
	return m.UnlockContext(context.Background())
}

// UnlockContext unlocks m and returns the status of unlock.
func (m *Mutex) UnlockContext(ctx context.Context) (bool, error) {
	n, err := m.actOnPoolsAsync(func(pool redis.Pool) (bool, error) {
		return m.release(ctx, pool, m.value)
	})
	if n < m.quorum {
		return false, err
	}
	return true, nil
}

// Extend resets the mutex's expiry and returns the status of expiry extension.
func (m *Mutex) Extend() (bool, error) {
	return m.ExtendContext(context.Background())
}

// ExtendContext resets the mutex's expiry and returns the status of expiry extension.
func (m *Mutex) ExtendContext(ctx context.Context) (bool, error) {
	start := time.Now()
	n, err := m.actOnPoolsAsync(func(pool redis.Pool) (bool, error) {
		return m.touch(ctx, pool, m.value, int(m.expiry/time.Millisecond))
	})
	if n < m.quorum {
		return false, err
	}
	now := time.Now()
	until := now.Add(m.expiry - now.Sub(start) - time.Duration(int64(float64(m.expiry)*m.driftFactor)))
	if now.Before(until) {
		m.until = until
		return true, nil
	}
	return false, ErrExtendFailed
}

// Valid returns true if the lock acquired through m is still valid. It may
// also return true erroneously if quorum is achieved during the call and at
// least one node then takes long enough to respond for the lock to expire.
//
// Deprecated: Use Until instead. See https://github.com/go-redsync/redsync/issues/72.
func (m *Mutex) Valid() (bool, error) {
	return m.ValidContext(context.Background())
}

// ValidContext returns true if the lock acquired through m is still valid. It may
// also return true erroneously if quorum is achieved during the call and at
// least one node then takes long enough to respond for the lock to expire.
//
// Deprecated: Use Until instead. See https://github.com/go-redsync/redsync/issues/72.
func (m *Mutex) ValidContext(ctx context.Context) (bool, error) {
	n, err := m.actOnPoolsAsync(func(pool redis.Pool) (bool, error) {
		return m.valid(ctx, pool)
	})
	return n >= m.quorum, err
}

func (m *Mutex) valid(ctx context.Context, pool redis.Pool) (bool, error) {
	if m.value == "" {
		return false, nil
	}
	conn, err := pool.Get(ctx)
	if err != nil {
		return false, err
	}
	defer conn.Close()
	reply, err := conn.Get(m.name)
	if err != nil {
		return false, err
	}
	return m.value == reply, nil
}

func genValue() (string, error) {
	b := make([]byte, 16)
	_, err := rand.Read(b)
	if err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(b), nil
}

func (m *Mutex) acquire(ctx context.Context, pool redis.Pool, value string) (bool, error) {
	// 获取连接池的上下文
	conn, err := pool.Get(ctx)
	if err != nil {
		return false, err
	}
	defer conn.Close() // 关闭连接
	// 新增键值对,返回bool值
	reply, err := conn.SetNX(m.name, value, m.expiry)
	if err != nil {
		return false, err
	}
	return reply, nil
}

var deleteScript = redis.NewScript(1, `
	local val = redis.call("GET", KEYS[1])
	if val == ARGV[1] then
		return redis.call("DEL", KEYS[1])
	elseif val == false then
		return -1
	else
		return 0
	end
`)

func (m *Mutex) release(ctx context.Context, pool redis.Pool, value string) (bool, error) {
	conn, err := pool.Get(ctx)
	if err != nil {
		return false, err
	}
	defer conn.Close()
	status, err := conn.Eval(deleteScript, m.name, value)
	if err != nil {
		return false, err
	}
	if status == int64(-1) {
		return false, ErrLockAlreadyExpired
	}
	return status != int64(0), nil
}

// 如果键值对的值等于ARGV[1]，就设置过期时间为ARGV[2]
// 如果不相等就设置新值为ARGV[1]和过期时间为ARGV[2]
// 如果设置失败，则返回 0
var touchWithSetNXScript = redis.NewScript(1, `
	if redis.call("GET", KEYS[1]) == ARGV[1] then
		return redis.call("PEXPIRE", KEYS[1], ARGV[2])
	elseif redis.call("SET", KEYS[1], ARGV[1], "PX", ARGV[2], "NX") then
		return 1
	else
		return 0
	end
`)

// 设置过期时间为ARGV[2]（毫秒级别）
var touchScript = redis.NewScript(1, `
	if redis.call("GET", KEYS[1]) == ARGV[1] then
		return redis.call("PEXPIRE", KEYS[1], ARGV[2])
	else
		return 0
	end
`)

func (m *Mutex) touch(ctx context.Context, pool redis.Pool, value string, expiry int) (bool, error) {
	conn, err := pool.Get(ctx)
	if err != nil {
		return false, err
	}
	defer conn.Close()

	touchScript := touchScript
	if m.setNXOnExtend {
		touchScript = touchWithSetNXScript
	}

	status, err := conn.Eval(touchScript, m.name, value, expiry)
	if err != nil {
		return false, err
	}
	return status != int64(0), nil
}

// actFn为{bool, error}
func (m *Mutex) actOnPoolsAsync(actFn func(redis.Pool) (bool, error)) (int, error) {
	type result struct {
		node     int
		statusOK bool
		err      error
	}
	// 新建一个chan管道，大小为redis连接池大小
	ch := make(chan result, len(m.pools))
	// 存储redis连接池中节点的状态
	for node, pool := range m.pools {
		go func(node int, pool redis.Pool) {
			r := result{node: node}
			r.statusOK, r.err = actFn(pool)
			ch <- r
		}(node, pool)
	}

	var (
		n     = 0
		taken []int
		err   error
	)

	for range m.pools {
		r := <-ch
		if r.statusOK {
			n++
		} else if r.err == ErrLockAlreadyExpired { // 无法解锁，锁已经过期
			err = multierror.Append(err, ErrLockAlreadyExpired)
		} else if r.err != nil {
			err = multierror.Append(err, &RedisError{Node: r.node, Err: r.err})
		} else {
			taken = append(taken, r.node)
			err = multierror.Append(err, &ErrNodeTaken{Node: r.node})
		}
		// 是否快速失败，如果设置为 true，在某些条件下会立即返回失败，而不是进行重试。
		if m.failFast {
			// fast return
			// m.quorum 多节点分布式锁的法定人数，通常是集群中需要同意锁操作的节点数量。
			if n >= m.quorum {
				return n, err
			}

			// fail fast
			if len(taken) >= m.quorum {
				return n, &ErrTaken{Nodes: taken}
			}
		}
	}

	if len(taken) >= m.quorum {
		return n, &ErrTaken{Nodes: taken}
	}
	return n, err
}
