package main

import (
	"bytes"
	"context"
	"errors"
	"log"
	"net"
	"os"
	"time"
)

type Listener struct {
	addr string
	rawl net.Listener
	cb *ConnectionHandler
}

func NewListener(addr string) (*Listener, error) {
	rawl, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("listen addr: %v error: %v\n", addr, err)
		return nil, err
	}
	return &Listener{
		addr: addr,
		rawl: rawl,
	}, nil
}

func (l Listener) Accept() (net.Conn, error) {
	return l.rawl.Accept()
}

func (l *Listener) StopAccept() {
	tcpListener, ok := l.rawl.(*net.TCPListener)
	if !ok {
		return
	}
	err := tcpListener.SetDeadline(time.Now())
	if err != nil {
		log.Printf("[ERROR] [Listener] [StopAccept] error: %v, addr: %v\n", err, tcpListener.Addr())
	}
}

func (l *Listener) ListenerFile() (*os.File, error) {
	tcpListener, ok := l.rawl.(*net.TCPListener)
	if !ok {
		return nil, errors.New("raw listener is not TCPListener")
	}
	return tcpListener.File()
}

func (l *Listener) Close() error {
	return l.rawl.Close()
}

func (l *Listener) Addr() net.Addr {
	return l.rawl.Addr()
}
func (l *Listener) SetCB(cb *ConnectionHandler) {
	l.cb = cb
}

func (l *Listener) GetCB() *ConnectionHandler {
	return l.cb
}

func mergeListener(addr string, inheritListeners []net.Listener) (*Listener, error) {

	tcpAddr, err := net.ResolveTCPAddr("tcp", addr)
	if err != nil {
		log.Printf("[ERROR]")
		return nil, err
	}

	for i, il := range inheritListeners {
		if _, ok := il.(*net.TCPListener); !ok {
			continue
		}
		if il.Addr().String() != tcpAddr.String() {
			continue
		}
		inheritListeners[i] = nil
		return &Listener{
			addr: il.Addr().String(),
			rawl: il,
		}, nil
	}

	l, err := NewListener(addr)
	if err != nil {
		return nil, err
	}
	return l, nil
}

type ConnectionHandler struct {
	listeners []*Listener
	conns     []*connection
	stopChan chan struct{} // stop connection read form socket
}

func (ch *ConnectionHandler) AddListener(l *Listener) {
	l.SetCB(ch)
	ch.listeners = append(ch.listeners, l)
}

func (ch *ConnectionHandler) StartServers(ctx context.Context, stopChan chan struct{}) {
	ch.startServers(ctx)
	ch.stopChan = stopChan
}

func (ch *ConnectionHandler) startServers(ctx context.Context) {
	for _, l := range ch.listeners {
		go func(l *Listener) {
			log.Printf("[INFO] [startServers] server starting, addr: %v\n", l.Addr())
			for {
				select {
				case <-ctx.Done():
					log.Printf("[INFO] [startServers] listener exit, listener addr: %v\n", l.Addr())
					return
				default:
					conn, err := l.Accept()
					if err == nil {
						go ch.OnAccept(conn, nil, nil)
					} else {
						if nerr, ok := err.(net.Error); ok && nerr.Timeout() {
							log.Printf("[INFO] [startServers] listener stop accepting connections by deadline, listener addr: %v\n", l.Addr())
							return
						} else if ope, ok := err.(*net.OpError); ok {
							// not timeout error and not temporary, which means the error is non-recoverable
							// stop accepting loop and log the event
							if !(ope.Timeout() && ope.Temporary()) {
								// accept error raised by sockets closing
								if ope.Op == "accept" {
									log.Printf("[INFO] [startServers] listener closed, addr is: : %v\n", l.Addr())
								} else {
									log.Printf("[INFO] [startServers] listener occurs non-recoverable error, stop listening and accepting. error: %v addr: %v\n",
										err, l.Addr())
								}
								return
							}
						} else {
							log.Printf("[INFO] [startServers]  listener occurs unknown error while accepting, error:%s addr: %v\n", err, l.Addr())
						}
					}
				}
			}
		}(l)
	}
}

func (ch *ConnectionHandler) FindListenerByAddress(addr net.Addr) *Listener {
	for _, l := range ch.listeners {
		if l.rawl != nil {
			if l.rawl.Addr().Network() == addr.Network() &&
				l.rawl.Addr().String() == addr.String() {
				return l
			}
		}
	}
	return nil
}

func (ch *ConnectionHandler) listListenerFiles() ([]*os.File, error) {
	fs := make([]*os.File, 0, len(ch.listeners))
	for _, l := range ch.listeners {
		file, err := l.ListenerFile()
		if err != nil {
			return nil, err
		}
		fs = append(fs, file)
	}
	return fs, nil
}

func (ch *ConnectionHandler) OnAccept(rawc net.Conn, transferCh chan *connection, buf []byte) {
	ctx := context.TODO()
	if transferCh != nil {
		ctx = context.WithValue(ctx, ContextKeyAcceptChan, transferCh)
		ctx = context.WithValue(ctx, ContextKeyAcceptBuffer, buf)
	}

	conn := NewServerConnection(ctx, rawc, ch.stopChan, ch)
	conn.SetEnableTransfer(true)
	conn.Start()
}

func (ch *ConnectionHandler) Handle(c *connection, buf *bytes.Buffer, len int) {
	time.Sleep(time.Second * 5)
	data := buf.String()
	log.Printf("[INFO] revice form addr: %v, data: %v\n",c.rawConnection.RemoteAddr(), data)
	buf.Reset()
	data = "server: " + data
	c.Write(bytes.NewBufferString(data))
}

func (ch *ConnectionHandler) StopAccept() {
	for _, l := range ch.listeners {
		l.StopAccept()
	}
}

func (ch *ConnectionHandler) StopConnection() {
	close(ch.stopChan)
}