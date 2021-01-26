package main

import (
	"errors"
	"log"
	"net"
	"os"
	"time"
)

type Listener struct {
	addr string
	rawl net.Listener
}

func NewListener(addr string) (*Listener, error) {
	rawl, err := net.Listen("tcp", addr)
	if err != nil {
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
	log.Printf("[INFO] [Listener] [StopAccept] addr: %v\n", tcpListener.Addr())
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

func (l Listener) Addr() net.Addr {
	return l.rawl.Addr()
}

func mergeListener(addr string, inheritListeners []net.Listener) (*Listener, error) {

	tcpAddr , err := net.ResolveTCPAddr("tcp", addr)
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
