package pool

import (
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

var (
	//ErrMaxActiveConnReached 连接池超限
	ErrMaxActiveConnReached = errors.New("MaxActiveConnReached")
)

// Config 连接池相关配置
type Config struct {
	//连接池中拥有的最小连接数
	InitialCap int
	//最大并发存活连接数
	MaxCap int
	//最大空闲连接
	MaxIdle int
	//生成连接的方法
	Factory func() (net.Conn, error)
	//关闭连接的方法
	Close func(conn net.Conn) error
	//检查连接是否有效的方法
	Ping func(conn net.Conn) error
	//连接最大空闲时间，超过该事件则将失效
	IdleTimeout time.Duration
	//连接最大存活时间，超过该时间则关闭
	LifeTime time.Duration
}

// channelPool 存放连接信息
type channelPool struct {
	mu                    sync.RWMutex
	conns                 chan *idleConn
	factory               func() (net.Conn, error)
	close                 func(conn net.Conn) error
	ping                  func(conn net.Conn) error
	idleTimeout, lifeTime time.Duration
	maxActive             int32
	openingConns          int32
	statusChangedSignal   chan struct{}
}

type idleConn struct {
	net.Conn
	t    time.Time
	born time.Time
}

// NewChannelPool 初始化连接
func NewChannelPool(poolConfig *Config) (Pool, error) {
	if !(poolConfig.InitialCap <= poolConfig.MaxIdle /*&& poolConfig.MaxCap >= poolConfig.MaxIdle*/ && poolConfig.InitialCap >= 0) {
		return nil, errors.New("invalid capacity settings")
	}
	if poolConfig.Factory == nil {
		return nil, errors.New("invalid factory func settings")
	}
	if poolConfig.Close == nil {
		return nil, errors.New("invalid close func settings")
	}

	c := &channelPool{
		conns:               make(chan *idleConn, poolConfig.MaxIdle),
		factory:             poolConfig.Factory,
		close:               poolConfig.Close,
		idleTimeout:         poolConfig.IdleTimeout,
		lifeTime:            poolConfig.LifeTime,
		maxActive:           int32(poolConfig.MaxCap),
		openingConns:        int32(poolConfig.InitialCap),
		statusChangedSignal: make(chan struct{}),
	}

	if poolConfig.Ping != nil {
		c.ping = poolConfig.Ping
	}

	for i := 0; i < poolConfig.InitialCap; i++ {
		conn, err := c.factory()
		if err != nil {
			c.Release()
			return nil, fmt.Errorf("factory is not able to fill the pool: %s", err)
		}
		c.conns <- &idleConn{Conn: conn, born: time.Now(), t: time.Now()}
	}

	return c, nil
}

// getConns 获取所有连接
func (c *channelPool) getConns() chan *idleConn {
	c.mu.Lock()
	conns := c.conns
	c.mu.Unlock()
	return conns
}

// Get 从pool中取一个连接
func (c *channelPool) Get() (net.Conn, error) {
BEGIN:
	conns := c.getConns()
	if conns == nil {
		return nil, ErrClosed
	}
	for {
		var wrapConn *idleConn
		select {
		case wrapConn = <-conns:
		default:
			c.mu.Lock()
			openingConns := atomic.LoadInt32(&c.openingConns)
			if openingConns >= atomic.LoadInt32(&c.maxActive) {
				if c.statusChangedSignal == nil {
					c.statusChangedSignal = make(chan struct{})
				}
				statusCh := c.statusChangedSignal
				c.mu.Unlock()
				select {
				case wrapConn = <-conns:
					goto GOTCONN
				case <-statusCh:
					goto BEGIN
				}
			}
			if c.factory == nil {
				c.mu.Unlock()
				return nil, ErrClosed
			}
			conn, err := c.factory()
			if err != nil {
				c.mu.Unlock()
				return nil, err
			}
			wrapConn = &idleConn{Conn: conn, born: time.Now(), t: time.Now()}
			atomic.AddInt32(&c.openingConns, 1)
			c.mu.Unlock()
			return wrapConn, nil
		}

	GOTCONN:
		if wrapConn == nil {
			return nil, ErrClosed
		}
		//判断是否超时，超时则丢弃
		if timeout := c.idleTimeout; timeout > 0 {
			if wrapConn.t.Add(timeout).Before(time.Now()) {
				//丢弃并关闭该连接
				c.Close(wrapConn)
				continue
			}
		}
		//判断是否过期，过期则丢弃
		if lifetime := c.lifeTime; lifetime > 0 {
			if wrapConn.born.Add(lifetime).Before(time.Now()) {
				c.Close(wrapConn)
				continue
			}
		}
		//判断是否失效，失效则丢弃，如果用户没有设定 ping 方法，就不检查
		if c.ping != nil {
			if err := c.Ping(wrapConn); err != nil {
				c.Close(wrapConn)
				continue
			}
		}
		return wrapConn, nil
	}
}

// Put 将连接放回pool中
func (c *channelPool) Put(conn net.Conn) error {
	if conn == nil {
		return errors.New("connection is nil. rejecting")
	}

	c.mu.Lock()

	if c.conns == nil {
		c.mu.Unlock()
		return c.Close(conn)
	}

	wrapConn, ok := conn.(*idleConn)
	if !ok {
		wrapConn = &idleConn{Conn: conn, t: time.Now(), born: time.Now()}
	} else {
		wrapConn.t = time.Now()
	}

	select {
	case c.conns <- wrapConn:
		c.mu.Unlock()
		return nil
	default:
		c.mu.Unlock()
		//连接池已满，直接关闭该连接
		return c.Close(conn)
	}
}

// Close 关闭单条连接
func (c *channelPool) Close(conn net.Conn) error {
	if conn == nil {
		return errors.New("connection is nil. rejecting")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.close == nil {
		return nil
	}
	atomic.AddInt32(&c.openingConns, -1)
	c.signalStatusChanged()
	return c.close(conn)
}

// Ping 检查单条连接是否有效
func (c *channelPool) Ping(conn net.Conn) error {
	if conn == nil {
		return errors.New("connection is nil. rejecting")
	}
	return c.ping(conn)
}

// Release 释放连接池中所有连接
func (c *channelPool) Release() {
	c.mu.Lock()
	conns := c.conns
	c.conns = nil
	c.factory = nil
	c.ping = nil
	closeFun := c.close
	c.close = nil
	c.signalStatusChanged()
	c.mu.Unlock()

	if conns == nil {
		return
	}

	close(conns)
	for wrapConn := range conns {
		//log.Printf("Type %v\n",reflect.TypeOf(wrapConn.conn))
		closeFun(wrapConn)
	}

}

// Len 连接池中已有的连接
func (c *channelPool) Len() int {
	return len(c.getConns())
}

// Opening 返回打开的连接数
func (c *channelPool) Opening() int {
	return int(atomic.LoadInt32(&c.openingConns))
}

// SetMaxCap 调整最大连接数
func (c *channelPool) SetMaxCap(maxCap int) error {
	old := atomic.SwapInt32(&c.maxActive, int32(maxCap))
	if old >= int32(maxCap) {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.signalStatusChanged()
	return nil
}

// GetMaxCap 返回最大连接数
func (c *channelPool) GetMaxCap() int {
	return int(atomic.LoadInt32(&c.maxActive))
}

func (c *channelPool) signalStatusChanged() {
	if c.statusChangedSignal == nil {
		return
	}
	close(c.statusChangedSignal)
	c.statusChangedSignal = nil
}
