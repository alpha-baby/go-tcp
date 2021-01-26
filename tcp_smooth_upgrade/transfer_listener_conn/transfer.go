package main

import (
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"
	"runtime/debug"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

var (
	ReconfigureDomainSocket    = "listener.sock"
	TransferListenDomainSocket = "listener.sock"
	TransferConnDomainSocket = "conn.sock"
)

func init() {
	absPath, _ := filepath.Abs(os.Args[0])
	execPath := filepath.Dir(absPath)
	ReconfigureDomainSocket = filepath.Join(execPath, ReconfigureDomainSocket)
	TransferListenDomainSocket = filepath.Join(execPath, TransferListenDomainSocket)
	TransferConnDomainSocket = filepath.Join(execPath, TransferConnDomainSocket)
}

func ReconfigureHandler() {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("[ERROR] [transfer] [ReconfigureHandler] panic %v\n%s\n", r, string(debug.Stack()))
			// todo restart goroutine
		}
	}()
	syscall.Unlink(ReconfigureDomainSocket)

	l, err := net.Listen("unix", ReconfigureDomainSocket)
	if err != nil {
		log.Printf("[ERROR] [transfer] [ReconfigureHandler] net listen error: %v\n", err)
		return
	}
	defer l.Close()

	log.Printf("[INFO] [transfer] [ReconfigureHandler] start\n")

	ul := l.(*net.UnixListener)
	for {
		uc, err := ul.AcceptUnix()
		if err != nil {
			log.Printf("[ERROR] [transfer] [ReconfigureHandler] Accept Unix Connnection error :%v\n", err)
			return
		}
		log.Printf("[INFO] [transfer] [ReconfigureHandler] Accept new process coming\n")

		_, err = uc.Write([]byte{0})
		if err != nil {
			log.Printf("[ERROR] [transfer] [ReconfigureHandler] ack to new proces error: %v\n", err)
			continue
		}
		uc.Close()

		reconfigure()
	}
}

func reconfigure() {
	// todo set process stat,
	// transfer listen fd
	var listenSockConn net.Conn
	var err error
	var n int
	var buf [1]byte
	if listenSockConn, err = sendInheritListeners(); err != nil {
		return
	}

	// Wait new Process ack
	listenSockConn.SetReadDeadline(time.Now().Add(10 * time.Minute))
	n, err = listenSockConn.Read(buf[:])
	if n != 1 {
		log.Printf("[ERROR] [transfer] [reconfigure] new process start failed\n")
		return
	}
	// Wait for new mosn start
	time.Sleep(3 * time.Second)

	// Stop accepting requests
	StopAccept()
	// Wait for all connections to be finished
	WaitConnectionsDone(30 * time.Second)

	log.Printf("[INFO] [transfer] [reconfigure] new process started, old process exit!\n")
	os.Exit(0)
}

func sendInheritListeners() (net.Conn, error) {
	lf, err := connhandler.listListenerFiles()
	if err != nil {
		return nil, fmt.Errorf("ListListenersFile() error: %v\n", err)
	}

	var files []*os.File
	files = append(files, lf...)

	fds := make([]int, len(files))
	for i, f := range files {
		fds[i] = int(f.Fd())
		defer f.Close()
	}

	var unixConn net.Conn
	// retry 10 time
	for i := 0; i < 10; i++ {
		unixConn, err = net.DialTimeout("unix", TransferListenDomainSocket, 1*time.Second)
		if err == nil {
			break
		}
		time.Sleep(1 * time.Second)
	}
	if err != nil {
		log.Printf("[ERROR] [transfer] [sendInheritListeners] Dial unix failed %v\n", err)
		return nil, err
	}

	uc := unixConn.(*net.UnixConn)
	buf := make([]byte, 1)
	rights := syscall.UnixRights(fds...)
	n, oobn, err := uc.WriteMsgUnix(buf, rights, nil)
	if err != nil {
		log.Printf("[ERROR] [transfer] [sendInheritListeners] WriteMsgUnix error: %v\n", err)
		return nil, err
	}
	if n != len(buf) || oobn != len(rights) {
		log.Printf("[ERROR] [transfer] [sendInheritListeners] WriteMsgUnix = %d, %d; want 1, %d\n", n, oobn, len(rights))
		return nil, err
	}

	return uc, nil
}

func GetInheritListeners() ([]net.Listener, net.Conn, error) {
	if !isReconfigure() { // 判断是否有老进程存在
		return nil, nil, nil
	}

	syscall.Unlink(TransferListenDomainSocket)

	l, err := net.Listen("unix", TransferListenDomainSocket)
	if err != nil {
		log.Printf("[ERROR] InheritListeners net listen error: %v", err)
		return nil, nil, err
	}
	defer l.Close()

	log.Printf("[INFO] Get InheritListeners start")

	ul := l.(*net.UnixListener)
	ul.SetDeadline(time.Now().Add(time.Second * 10))
	uc, err := ul.AcceptUnix()
	if err != nil {
		log.Printf("[ERROR] InheritListeners Accept error :%v", err)
		return nil, nil, err
	}
	log.Printf("[INFO] Get InheritListeners Accept")

	buf := make([]byte, 1)
	oob := make([]byte, 1024)
	_, oobn, _, _, err := uc.ReadMsgUnix(buf, oob)
	if err != nil {
		return nil, nil, err
	}
	scms, err := unix.ParseSocketControlMessage(oob[0:oobn])
	if err != nil {
		log.Printf("[ERROR] ParseSocketControlMessage: %v", err)
		return nil, nil, err
	}
	if len(scms) != 1 {
		log.Printf("[ERROR] expected 1 SocketControlMessage; got scms = %#v", scms)
		return nil, nil, err
	}
	gotFds, err := unix.ParseUnixRights(&scms[0])
	if err != nil {
		log.Printf("[ERROR] unix.ParseUnixRights: %v", err)
		return nil, nil, err
	}

	var listeners []net.Listener
	for i := 0; i < len(gotFds); i++ {
		fd := uintptr(gotFds[i])
		file := os.NewFile(fd, "")
		if file == nil {
			log.Printf("[ERROR] create new file from fd %d failed", fd)
			return nil, nil, err
		}
		defer file.Close()

		fileListener, err := net.FileListener(file)
		if err != nil {
			log.Printf("[ERROR] recover listener from fd %d failed: %s", fd, err)
			return nil, nil, err
		}
		// for tcp or unix listener
		listeners = append(listeners, fileListener)
	}

	return listeners, uc, nil
}

func isReconfigure() bool {
	var unixConn net.Conn
	var err error
	unixConn, err = net.DialTimeout("unix", ReconfigureDomainSocket, 1*time.Second)
	if err != nil {
		log.Printf("[INFO] [transfer] [isReconfigure] not reconfigure: %v\n", err)
		return false
	}
	defer unixConn.Close()

	uc := unixConn.(*net.UnixConn)
	buf := make([]byte, 1)
	n, _ := uc.Read(buf)
	if n != 1 {
		return false
	}
	return true
}
