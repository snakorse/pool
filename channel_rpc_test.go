package pool

import (
	"bufio"
	"io"
	"math/rand"
	"net"
	"net/http"
	"net/rpc"
	"sync"
	"testing"
	"time"
	"unsafe"
)

var (
	InitialCap = 5
	MaxIdleCap = 10
	MaximumCap = 100
	network    = "tcp"
	address    = "127.0.0.1:17777"
	factory    = func() (net.Conn, error) { return net.Dial(network, address) }
	closeFac   = func(v net.Conn) error {
		return v.Close()
	}
)

func init() {
	// used for factory function
	go rpcServer()
	time.Sleep(time.Millisecond * 300) // wait until tcp server has been settled

	rand.Seed(time.Now().UTC().UnixNano())
}

func TestNew(t *testing.T) {
	p, err := newChannelPool()
	defer p.Release()
	if err != nil {
		t.Errorf("New error: %s", err)
	}
}
func TestPool_Get_Impl(t *testing.T) {
	p, _ := newChannelPool()
	defer p.Release()

	conn, err := p.Get()
	if err != nil {
		t.Errorf("Get error: %s", err)
	}
	_, ok := conn.(*idleConn)
	if !ok {
		t.Errorf("Conn is not of type poolConn")
	}
	p.Put(conn)
}

func TestPool_LifeTime(t *testing.T) {
	cfg := Config{InitialCap: 1, MaxCap: 2, MaxIdle: 2, Factory: factory, Close: closeFac, LifeTime: time.Second}
	p, err := NewChannelPool(&cfg)
	if err != nil {
		t.Error(err)
		return
	}

	conn, err := p.Get()
	if err != nil {
		t.Error(err)
		return
	}
	err = p.Put(conn)
	if err != nil {
		t.Error(err)
		return
	}

	conn1, err := p.Get()
	if err != nil {
		t.Error(err)
		return
	}
	err = p.Put(conn)
	if err != nil {
		t.Error(err)
		return
	}
	if unsafe.Pointer(conn.(*idleConn)) != unsafe.Pointer(conn1.(*idleConn)) {
		t.Errorf("should get same conn")
	}

	time.Sleep(time.Second * 2)

	conn2, err := p.Get()
	if err != nil {
		t.Error(err)
		return
	}
	if unsafe.Pointer(conn.(*idleConn)) == unsafe.Pointer(conn2.(*idleConn)) {
		t.Errorf("should not get expired conn")
	}
}

func TestPool_Get(t *testing.T) {
	p, _ := newChannelPool()
	defer p.Release()

	_, err := p.Get()
	if err != nil {
		t.Errorf("Get error: %s", err)
	}

	// after one get, current capacity should be lowered by one.
	if p.Len() != (InitialCap - 1) {
		t.Errorf("Get error. Expecting %d, got %d",
			(InitialCap - 1), p.Len())
	}

	if p.Opening() != InitialCap {
		t.Errorf("Opening error. Expecting %d, but got %d", InitialCap, p.Opening())
	}

	// get them all
	var wg sync.WaitGroup
	for i := 0; i < (MaximumCap - 1); i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := p.Get()
			if err != nil {
				t.Errorf("Get error: %s", err)
			}
		}()
	}
	wg.Wait()

	if p.Len() != 0 {
		t.Errorf("Get error. Expecting %d, got %d",
			0, p.Len())
	}

	if p.Opening() != MaximumCap {
		t.Errorf("Opening error. Expecting %d, but got %d", MaximumCap, p.Opening())
	}

	var conn interface{}
	go func() {
		conn, _ = p.Get()
	}()

	time.Sleep(time.Second * 5)
	if conn != nil {
		t.Errorf("expect Get blocking, but return not nil conn")
	}
}

func TestPool_Put(t *testing.T) {
	pconf := Config{InitialCap: InitialCap, MaxCap: MaximumCap, Factory: factory, Close: closeFac, IdleTimeout: time.Second * 20,
		MaxIdle: MaxIdleCap}
	p, err := NewChannelPool(&pconf)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Release()

	// get/create from the pool
	conns := make([]net.Conn, MaximumCap)
	for i := 0; i < MaximumCap; i++ {
		conn, _ := p.Get()
		conns[i] = conn
	}

	// now put them all back
	for _, conn := range conns {
		p.Put(conn)
	}

	if p.Len() != MaxIdleCap {
		t.Errorf("Put error len. Expecting %d, got %d",
			1, p.Len())
	}

	p.Release() // close pool

}

func TestPool_UsedCapacity(t *testing.T) {
	p, _ := newChannelPool()
	defer p.Release()

	if p.Len() != InitialCap {
		t.Errorf("InitialCap error. Expecting %d, got %d",
			InitialCap, p.Len())
	}
}

func TestPool_Close(t *testing.T) {
	p, _ := newChannelPool()

	// now close it and test all cases we are expecting.
	p.Release()

	c := p.(*channelPool)

	if c.conns != nil {
		t.Errorf("Close error, conns channel should be nil")
	}

	if c.factory != nil {
		t.Errorf("Close error, factory should be nil")
	}

	_, err := p.Get()
	if err == nil {
		t.Errorf("Close error, get conn should return an error")
	}

	if p.Len() != 0 {
		t.Errorf("Close error used capacity. Expecting 0, got %d", p.Len())
	}
}

func TestPoolConcurrent(t *testing.T) {
	p, _ := newChannelPool()
	pipe := make(chan net.Conn, 0)

	go func() {
		p.Release()
	}()

	for i := 0; i < 10000; i++ {
		go func() {
			conn, _ := p.Get()

			pipe <- conn
		}()

		go func() {
			conn := <-pipe
			if conn == nil {
				return
			}
			p.Put(conn)
		}()

		n := i
		go func() {
			p.SetMaxCap(10000 + n)
		}()
	}
}

func TestPoolWriteRead(t *testing.T) {
	//p, _ := NewChannelPool(0, 30, factory)
	p, _ := newChannelPool()
	conn, _ := p.Get()

	io.WriteString(conn, "CONNECT "+rpc.DefaultRPCPath+" HTTP/1.0\n\n")
	// Require successful HTTP response
	// before switching to RPC protocol.
	resp, err := http.ReadResponse(bufio.NewReader(conn), &http.Request{Method: "CONNECT"})
	if err != nil || resp.Status != "200 Connected to Go RPC" {
		t.Errorf("dail http rpc failed, err: %s", err)
		return
	}
	cli := rpc.NewClient(conn)

	var result int
	err = cli.Call("Arith.Multiply", Args{1, 2}, &result)
	if err != nil {
		t.Error(err)
	}
	if result != 2 {
		t.Error("rpc.err")
	}
}

func TestPoolConcurrent2(t *testing.T) {
	//p, _ := NewChannelPool(0, 30, factory)
	p, _ := newChannelPool()

	var wg sync.WaitGroup

	go func() {
		for i := 0; i < 10; i++ {
			wg.Add(1)
			go func(i int) {
				conn, _ := p.Get()
				time.Sleep(time.Millisecond * time.Duration(rand.Intn(100)))
				p.Close(conn)
				wg.Done()
			}(i)
		}
	}()

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(i int) {
			conn, _ := p.Get()
			time.Sleep(time.Millisecond * time.Duration(rand.Intn(100)))
			p.Close(conn)
			wg.Done()
		}(i)
	}

	wg.Wait()
}

func TestChannelPool_SetMaxCap(t *testing.T) {
	p, _ := newChannelPool()

	for i := 0; i < MaximumCap; i++ {
		_, _ = p.Get()
	}

	var conn interface{}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		conn, _ = p.Get()
		wg.Done()
	}()
	time.Sleep(time.Second * 5)
	if conn != nil {
		t.Errorf("Get error. Expect nil, but got not nil")
	}

	p.SetMaxCap(MaximumCap + 1)
	wg.Wait()
	if conn == nil {
		t.Errorf("Get error. Expect not nil, but got nil")
	}
}

//
//func TestPoolConcurrent3(t *testing.T) {
//	p, _ := NewChannelPool(0, 1, factory)
//
//	var wg sync.WaitGroup
//
//	wg.Add(1)
//	go func() {
//		p.Close()
//		wg.Done()
//	}()
//
//	if conn, err := p.Get(); err == nil {
//		conn.Close()
//	}
//
//	wg.Wait()
//}

func newChannelPool() (Pool, error) {
	pconf := Config{InitialCap: InitialCap, MaxCap: MaximumCap, Factory: factory, Close: closeFac, IdleTimeout: time.Second * 20,
		MaxIdle: MaxIdleCap}
	return NewChannelPool(&pconf)
}

func rpcServer() {
	arith := new(Arith)
	rpc.Register(arith)
	rpc.HandleHTTP()

	l, e := net.Listen("tcp", address)
	if e != nil {
		panic(e)
	}
	go func() {
		err := http.Serve(l, nil)
		if err != nil {
			panic(e)
		}
	}()
}

type Args struct {
	A, B int
}

type Arith int

func (t *Arith) Multiply(args *Args, reply *int) error {
	*reply = args.A * args.B
	return nil
}
