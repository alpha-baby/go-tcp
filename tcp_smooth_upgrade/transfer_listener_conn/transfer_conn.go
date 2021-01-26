package main

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"golang.org/x/sys/unix"
	"log"
	"net"
	"os"
	"runtime/debug"
	"strconv"
	"sync"
	"syscall"
	"time"
)
const (
	transferErr    = 0
	transferNotify = 1
)

// old mosn transfer readloop
func transferRead(c *connection) (uint64, error) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("[ERROR] [transfer] [read] panic %v\n%s", r, string(debug.Stack()))
		}
	}()
	unixConn, err := net.Dial("unix", TransferConnDomainSocket)
	if err != nil {
		log.Printf("[ERROR] [transfer] [read] net Dial unix failed c:%p, id:%d, err:%v", c, c.id, err)
		return transferErr, err
	}
	defer unixConn.Close()

	file, tlsConn, err := transferGetFile(c)
	if err != nil {
		log.Printf("[ERROR] [transfer] [read] transferRead failed: %v", err)
		return transferErr, err
	}

	uc := unixConn.(*net.UnixConn)
	// send type and TCP FD
	err = transferSendType(uc, file)
	if err != nil {
		log.Printf("[ERROR] [transfer] [read] transferRead failed: %v", err)
		return transferErr, err
	}
	// send header + buffer + TLS
	err = transferReadSendData(uc, tlsConn, c.readBuffer)
	if err != nil {
		log.Printf("[ERROR] [transfer] [read] transferRead failed: %v", err)
		return transferErr, err
	}
	// recv ID
	id := transferRecvID(uc)
	log.Printf("[INFO] [transfer] [read] TransferRead NewConn Id = %d, oldId = %d, %p, addrass = %s", id, c.id, c, c.rawConnection.RemoteAddr().String())

	return id, nil
}

// old mosn transfer writeloop
func transferWrite(c *connection, id uint64, buf *bytes.Buffer) error {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("[ERROR] [transfer] [write] transferWrite panic %v", r)
		}
	}()
	unixConn, err := net.Dial("unix", TransferConnDomainSocket)
	if err != nil {
		log.Printf("[ERROR] [transfer] [write] net Dial unix failed %v", err)
		return err
	}
	defer unixConn.Close()

	log.Printf("[INFO] [transfer] [write] TransferWrite id = %d, dataBuf = %d", id, buf.Len())
	uc := unixConn.(*net.UnixConn)
	err = transferSendType(uc, nil)
	if err != nil {
		log.Printf("[ERROR] [transfer] [write] transferWrite failed: %v", err)
		return err
	}
	
	// send header + buffer
	err = transferWriteSendData(uc, int(id), buf)
	if err != nil {
		log.Printf("[ERROR] [transfer] [write] transferWrite failed: %v", err)
		return err
	}
	return nil
}

func transferGetFile(c *connection) (file *os.File, tlsConn *net.Conn, err error) {
	switch conn := c.rawConnection.(type) {
	case *net.TCPConn:
		file, err = conn.File()
		if err != nil {
			return nil, nil, fmt.Errorf("TCP File failed %v", err)
		}
	default:
		return nil, nil, fmt.Errorf("unexpected net.Conn type; expected TCPConn or mtls.TLSConn, got %T", conn)
	}
	return
}

/**
 * type (1 bytes)
 *  0 : transfer read and FD
 *  1 : transfer write
 **/
func transferSendType(uc *net.UnixConn, file *os.File) error {
	buf := make([]byte, 1)
	// transfer write
	if file == nil {
		buf[0] = 1
		return transferSendMsg(uc, buf)
	}
	// transfer read, send FD
	return transferSendFD(uc, file)
}

func transferSendMsg(uc *net.UnixConn, b []byte) error {
	if len(b) == 0 {
		return nil
	}
	buf := bytes.NewBuffer(b)
	err := transferSendIoBuffer(uc, buf)
	if err != nil {
		return fmt.Errorf("transferSendMsg failed: %v", err)
	}
	return nil
}

func transferSendIoBuffer(uc *net.UnixConn, buf *bytes.Buffer) error {
	if buf.Len() == 0 {
		return nil
	}
	_, err := buf.WriteTo(uc)
	if err != nil {
		return fmt.Errorf("transferSendIobuffer failed: %v", err)
	}
	return nil
}

func transferSendFD(uc *net.UnixConn, file *os.File) error {
	buf := make([]byte, 1)
	// transfer read
	buf[0] = 0
	if file == nil {
		return errors.New("transferSendFD conn is nil")
	}
	defer file.Close()
	rights := syscall.UnixRights(int(file.Fd()))
	n, oobn, err := uc.WriteMsgUnix(buf, rights, nil)
	if err != nil {
		return fmt.Errorf("WriteMsgUnix: %v", err)
	}
	if n != len(buf) || oobn != len(rights) {
		return fmt.Errorf("WriteMsgUnix = %d, %d; want 1, %d", n, oobn, len(rights))
	}
	return nil
}

func transferReadSendData(uc *net.UnixConn, c *net.Conn, buf *bytes.Buffer) error {
	// send header
	s1 := buf.Len()
	s2 := 0
	err := transferSendHead(uc, uint32(s1), uint32(0))
	if err != nil {
		return err
	}
	log.Printf("[INFO] TransferRead dataBuf = %d, tlsBuf = %d", s1, s2)
	// send read/write buffer
	return transferSendIoBuffer(uc, buf)
}

func transferSendHead(uc *net.UnixConn, s1 uint32, s2 uint32) error {
	buf := transferBuildHead(s1, s2)
	return transferSendMsg(uc, buf)
}

func transferBuildHead(s1 uint32, s2 uint32) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint32(buf[0:], s1)
	binary.BigEndian.PutUint32(buf[4:], s2)
	return buf
}

func transferRecvID(uc *net.UnixConn) uint64 {
	b, err := transferRecvMsg(uc, 4)
	if err != nil {
		return transferErr
	}
	return uint64(binary.BigEndian.Uint32(b))
}

func transferRecvMsg(uc *net.UnixConn, size int) ([]byte, error) {
	if size == 0 {
		return nil, nil
	}
	b := make([]byte, size)
	var n, off int
	var err error
	for {
		n, err = uc.Read(b[off:])
		if err != nil {
			return nil, fmt.Errorf("transferRecvMsg failed: %v", err)
		}
		off += n
		if off == size {
			return b, nil
		}
	}
}

func transferWriteSendData(uc *net.UnixConn, id int, buf *bytes.Buffer) error {
	// send header
	err := transferSendHead(uc, uint32(buf.Len()), uint32(id))
	if err != nil {
		return err
	}
	// send read/write buffer
	return transferSendIoBuffer(uc, buf)
}
// ---- new process
func TransferServer(handler ConnectionHandler) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("[ERROR] [transfer] [server] transferServer panic %v", r)
		}
	}()


	syscall.Unlink(TransferConnDomainSocket)
	l, err := net.Listen("unix", TransferConnDomainSocket)
	if err != nil {
		log.Printf("[ERROR] [transfer] [server] transfer net listen error %v", err)
		return
	}
	defer l.Close()

	log.Printf("[INFO] [transfer] [server] TransferServer start")

	var transferMap sync.Map

	go func() {
		for {
			c, err := l.Accept()
			if err != nil {
				if ope, ok := err.(*net.OpError); ok && (ope.Op == "accept") {
					log.Printf("[INFO] [transfer] [server] TransferServer listener %s closed", l.Addr())
				} else {
					log.Printf("[ERROR] [transfer] [server] TransferServer Accept error :%v", err)
				}
				return
			}
			log.Printf("[INFO] [transfer] [server] transfer Accept")
			go transferHandler(c, handler, &transferMap)

		}
	}()

	<-time.After(2*TransferTimeout + 2*ConnReadTimeout + 10*time.Second)
	log.Printf("[INFO] [transfer] [server] TransferServer exit")
}

// transferHandler is called on recv transfer request
func transferHandler(c net.Conn, handler ConnectionHandler, transferMap *sync.Map) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("[ERROR] [transfer] [handler] transferHandler panic %v", r)
		}
	}()

	defer c.Close()

	uc, ok := c.(*net.UnixConn)
	if !ok {
		log.Printf("[ERROR] [transfer] [handler] unexpected FileConn type; expected UnixConn, got %T", c)
		return
	}
	// recv type
	conn, err := transferRecvType(uc)
	if err != nil {
		log.Printf("[ERROR] [transfer] [handler] transferRecvType error :%v", err)
		return
	}

	if conn != nil {
		// transfer read
		// recv header + buffer
		dataBuf, tlsBuf, err := transferReadRecvData(uc)
		if err != nil {
			log.Printf("[ERROR] [transfer] [handler] transferRecvData error :%v", err)
			return
		}
		connection := transferNewConn(conn, dataBuf, tlsBuf, handler, transferMap)
		if connection != nil {
			transferSendID(uc, connection.id)
		} else {
			transferSendID(uc, transferErr)
		}
	} else {
		// transfer write
		// recv header + buffer
		id, buf, err := transferWriteRecvData(uc)
		if err != nil {
			log.Printf("[ERROR] [transfer] [handler] transferRecvData error :%v", err)
		}
		connection := transferFindConnection(transferMap, uint64(id))
		if connection == nil {
			log.Printf("[ERROR] [transfer] [handler] transferFindConnection failed, id = %d", id)
			return
		}
		err = transferWriteBuffer(connection, buf)
		if err != nil {
			log.Printf("[ERROR] [transfer] [handler] transferWriteBuffer error :%v", err)
			return
		}
	}
}
func transferNewConn(conn net.Conn, dataBuf, tlsBuf []byte, handler ConnectionHandler, transferMap *sync.Map) *connection {

	listener := transferFindListen(conn.LocalAddr(), handler)
	if listener == nil {
		return nil
	}

	log.Printf("[INFO] [transfer] [new conn] transferNewConn dataBuf = %d, tlsBuf = %d", len(dataBuf), len(tlsBuf))


	ch := make(chan *connection, 1)
	// new connection
	go func() {
		// todo new connection
		listener.GetCB().OnAccept(conn, ch, dataBuf)
		log.Printf("[INFO] [transfer] [new conn], add new conn remote addr: %v\n", conn.RemoteAddr())
	}()

	select {
	// recv connection
	case conn := <-ch:

		log.Printf("[INFO] [transfer] [new conn] transfer NewConn id: %d", conn.id)
		transferMap.Store(conn.id, conn)
		return conn
	case <-time.After(3000 * time.Millisecond):
		log.Printf("[ERROR] [transfer] [new conn] transfer NewConn timeout, localAddress %+v, remoteAddress %+v", conn.LocalAddr(), conn.RemoteAddr())
		return nil
	}
}

func transferFindListen(addr net.Addr, handler ConnectionHandler) *Listener {
	address := addr.(*net.TCPAddr)
	port := strconv.FormatInt(int64(address.Port), 10)
	ipv4, _ := net.ResolveTCPAddr("tcp", "0.0.0.0:"+port)
	ipv6, _ := net.ResolveTCPAddr("tcp", "[::]:"+port)

	var listener *Listener
	if listener = handler.FindListenerByAddress(address); listener != nil {
		return listener
	} else if listener = handler.FindListenerByAddress(ipv4); listener != nil {
		return listener
	} else if listener = handler.FindListenerByAddress(ipv6); listener != nil {
		return listener
	}

	log.Printf("[ERROR] [transfer] Find Listener failed %v", address)
	return nil
}

func transferRecvType(uc *net.UnixConn) (net.Conn, error) {
	buf := make([]byte, 1)
	oob := make([]byte, 32)
	_, oobn, _, _, err := uc.ReadMsgUnix(buf, oob)
	if err != nil {
		return nil, fmt.Errorf("ReadMsgUnix error: %v", err)
	}
	// transfer write
	if buf[0] == 1 {
		return nil, nil
	}
	// transfer read, recv FD
	conn, err := transferRecvFD(oob[0:oobn])
	if err != nil {
		return nil, err
	}
	return conn, nil
}

func transferRecvFD(oob []byte) (net.Conn, error) {
	scms, err := unix.ParseSocketControlMessage(oob)
	if err != nil {
		return nil, fmt.Errorf("ParseSocketControlMessage: %v", err)
	}
	if len(scms) != 1 {
		return nil, fmt.Errorf("expected 1 SocketControlMessage; got scms = %#v", scms)
	}
	scm := scms[0]
	gotFds, err := unix.ParseUnixRights(&scm)
	if err != nil {
		return nil, fmt.Errorf("unix.ParseUnixRights: %v", err)
	}
	if len(gotFds) != 1 {
		return nil, fmt.Errorf("wanted 1 fd; got %#v", gotFds)
	}
	f := os.NewFile(uintptr(gotFds[0]), "fd-from-old")
	defer f.Close()
	conn, err := net.FileConn(f)
	if err != nil {
		return nil, fmt.Errorf("FileConn error :%v", gotFds)
	}
	return conn, nil
}

func transferReadRecvData(uc *net.UnixConn) ([]byte, []byte, error) {
	// recv header
	dataSize, tlsSize, err := transferRecvHead(uc)
	if err != nil {
		return nil, nil, err
	}
	// recv read buffer and TLS
	buf, err := transferRecvMsg(uc, dataSize+tlsSize)
	if err != nil {
		return nil, nil, err
	}

	return buf[0:dataSize], buf[dataSize:], nil
}

func transferRecvHead(uc *net.UnixConn) (int, int, error) {
	buf, err := transferRecvMsg(uc, 8)
	if err != nil {
		return 0, 0, fmt.Errorf("ReadMsgUnix error: %v", err)
	}
	size := int(binary.BigEndian.Uint32(buf[0:]))
	id := int(binary.BigEndian.Uint32(buf[4:]))
	return size, id, nil
}

func transferSendID(uc *net.UnixConn, id uint64) error {
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, uint32(id))
	return transferSendMsg(uc, b)
}


func transferWriteRecvData(uc *net.UnixConn) (int, []byte, error) {
	// recv header
	size, id, err := transferRecvHead(uc)
	if err != nil {
		return 0, nil, err
	}
	// recv write buffer
	buf, err := transferRecvMsg(uc, size)
	if err != nil {
		return 0, nil, err
	}

	return id, buf, nil
}

func transferFindConnection(transferMap *sync.Map, id uint64) *connection {
	conn, ok := transferMap.Load(id)
	if !ok {
		return nil
	}
	return conn.(*connection)
}

func transferWriteBuffer(conn *connection, buf []byte) error {
	log.Printf("[INFO] [transfer] revc writebuf: %v len: %v conn.id: %v\n", string(buf), len(buf), conn.id)
	iobuf := bytes.NewBuffer(buf)
	return conn.Write(iobuf)
}