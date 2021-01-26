package main

import (
	"bytes"
	"context"
	"io"
	"log"
	"math/rand"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

var idCounter uint64

// TransferTimeout is the total transfer time
var TransferTimeout = time.Second * 30
var DefaultConnWriteTimeout = time.Second * 15
var ConnReadTimeout      = 15 * time.Second

var (
	ContextKeyConnectionFd = "ContextKeyConnectionFd"
	ContextKeyAcceptChan   = "ContextKeyAcceptChan"
	ContextKeyAcceptBuffer = "ContextKeyAcceptBuffer"
)

type Connection interface {
	Start()
	SetEnableTransfer(enable bool)
}

type connection struct {
	id            uint64
	rawConnection net.Conn
	file          *os.File

	stopChan        chan struct{}
	readBuffer      *bytes.Buffer
	writeBuffers    net.Buffers
	ioBuffers       []bytes.Buffer
	writeBufferChan chan *bytes.Buffer
	transferChan    chan uint64

	startOnce      sync.Once
	enableTransfer bool

	handler *ConnectionHandler
}

func NewServerConnection(ctx context.Context, rawc net.Conn, stopChan chan struct{}, h *ConnectionHandler) *connection {
	id := atomic.AddUint64(&idCounter, 1)

	conn := &connection{
		id:            id,
		rawConnection: rawc,
		stopChan:      stopChan,
		writeBufferChan: make(chan *bytes.Buffer, 8),
		transferChan:     make(chan uint64),
		handler: h,
	}

	// store fd
	if val := ctx.Value(ContextKeyConnectionFd); val != nil {
		conn.file = val.(*os.File)
	}

	// transfer old mosn connection
	if val := ctx.Value(ContextKeyAcceptChan); val != nil {
		if val := ctx.Value(ContextKeyAcceptBuffer); val != nil {
			buf := val.([]byte)
			conn.readBuffer = bytes.NewBuffer(buf)
		}

		ch := val.(chan *connection)
		ch <- conn

		log.Printf("[INFO] [connection] [NewServerConnection] transfer conn from old process, id = %d, buffer = %d\n",
			conn.id, conn.readBuffer.Len())
	} else {
		log.Printf("[INFO] [connection] [NewServerConnection] new conn, id = %v\n", conn.id)
	}

	return conn
}

func (c *connection) Close() error {
	c.rawConnection.Close()
	c.readBuffer.Reset()
	c.file.Close()
	return nil
}

func (c *connection) Start() {
	if c.rawConnection.RemoteAddr() == nil {
		return
	}
	c.startOnce.Do(
		func() {
			go c.rLoop()
			go c.wLoop()
		},
	)
}

func (c *connection) rLoop() {
	var transferTime time.Time
	for {
		select {
		case <-c.stopChan:
			if transferTime.IsZero() {
				if c.enableTransfer {
					randTime := time.Duration(rand.Intn(int(TransferTimeout.Nanoseconds())))
					transferTime = time.Now().Add(TransferTimeout).Add(randTime)
					log.Printf("[INFO] [read loop] transferTime: Wait %d Second", (TransferTimeout+randTime)/1e9)
				} else {
					// set a long time, not transfer connection, wait mosn exit.
					transferTime = time.Now().Add(10 * TransferTimeout)
					log.Printf("[INFO] [read loop] not support transfer connection, Connection = %d, Local Address = %+v, Remote Address = %+v",
						c.id, c.rawConnection.LocalAddr(), c.rawConnection.RemoteAddr())
				}
			} else {
				if transferTime.Before(time.Now()) {
					c.transfer()
					return
				}
			}
		default:
		}
		err := c.doRead()
		if err != nil {
			if te, ok := err.(net.Error); ok && te.Timeout() {
				continue
			}

			if err == io.EOF {
				c.Close()
			} else {
				c.Close()
			}

			return
		}
	}
}

func (c *connection) doRead() error {

	buf := make([]byte, 1<<7)
	var  bytesRead int
	var err error
	c.rawConnection.SetReadDeadline(time.Now().Add(time.Second * 15))
	//bytesRead, err = c.readBuffer.ReadFrom(c.rawConnection)
	bytesRead, err = c.rawConnection.Read(buf)

	if err != nil {
		if te, ok := err.(net.Error); ok && te.Timeout() {
			if bytesRead == 0 {
				return err
			}
		} else if err != io.EOF {
			return err
		}
	}


	if bytesRead == 0 && err == nil {
		err = io.EOF
		log.Printf("[ERROR] [doRead] ReadOnce maybe always return (0, nil) and causes dead loop, Connection = %d, Local Address = %+v, Remote Address = %+v",
			c.id, c.rawConnection.LocalAddr(), c.rawConnection.RemoteAddr())
	}
	c.readBuffer = bytes.NewBuffer(buf[:bytesRead])

	// todo onReadEvent
	c.handler.Handle(c, c.readBuffer, int(bytesRead))
	return nil
}

func (c *connection) wLoop() {
	var needTransfer bool
	defer func() {
		if !needTransfer {
			close(c.writeBufferChan)
		}
	}()

	var err error
	for {
		select {

		case <-c.transferChan:
			needTransfer = true
			return
		case buf, ok := <-c.writeBufferChan:
			if !ok {
				return
			}

			c.rawConnection.SetWriteDeadline(time.Now().Add(DefaultConnWriteTimeout))
			_, err = c.doWrite(buf)
		}

		if err != nil {
			c.Close()
			return
		}
	}
}

func (c *connection) Write(buf *bytes.Buffer) (err error) {
	select {
	case c.writeBufferChan <- buf:
		return
	}

	return
}

func (c *connection) doWrite(buf *bytes.Buffer) (int, error) {
	bytesSent, err := buf.WriteTo(c.rawConnection)

	return int(bytesSent), err
}

func (c *connection) SetEnableTransfer(enable bool) {
	c.enableTransfer = enable
}

func (c *connection) transfer() {
	c.transferChan <- 1
	id, _ := transferRead(c)
	c.transferWrite(id)
}

func (c *connection) transferWrite(id uint64) {
	log.Printf("[INFO] TransferWrite begin")
	for {
		select {
		case buf, ok := <-c.writeBufferChan:
			if !ok {
				return
			}
			transferWrite(c, id, buf)
		}
	}
}