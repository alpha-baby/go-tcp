package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
)

var (
	Addrs []string
	stopChan = make(chan struct{})
	connhandler ConnectionHandler
)

func main() {
	Addrs = append(Addrs, "127.0.0.1:1234")
	Addrs = append(Addrs, "127.0.0.1:1235")
	ctx, cancel := context.WithCancel(context.TODO())
	inheritListeners, listenSockConn, err := GetInheritListeners()
	if err != nil {
		log.Printf("[mosn] [NewMosn] getInheritListeners failed, exit")
	}
	for _, addr := range Addrs {
		l, err := mergeListener(addr, inheritListeners)
		if err != nil {
			log.Printf("[WARN] mergeListener error: %v\n", err)
			continue
		}
		connhandler.AddListener(l)
	}

	go connhandler.StartServers(ctx, stopChan)
	if inheritListeners != nil {
		// ack old process start success
		if _, err := listenSockConn.Write([]byte{0}); err != nil {
			log.Fatalf("[new] process graceful failed, exit")
		}
		listenSockConn.Close()

		go TransferServer(connhandler)
	}
	// close inheritListener
	for _, ln := range inheritListeners {
		if ln != nil {
			log.Printf("[new] close useless legacy listener: %s", ln.Addr().String())
			ln.Close()
		}
	}

	sig := make(chan os.Signal)
	signal.Notify(sig, syscall.SIGINT)

	go ReconfigureHandler()

	select {
	case <-sig:
		cancel()
	}
}



func StopAccept() {
	connhandler.StopAccept()
}

func StopConnection() {
	connhandler.StopConnection()
}

// WaitConnectionsDone Wait for all connections to be finished
func WaitConnectionsDone(duration time.Duration) {
	// one duration wait for connection to active close
	// two duration wait for connection to transfer
	// DefaultConnReadTimeout wait for read timeout
	timeout := time.NewTimer(2*duration + 2*ConnReadTimeout)
	StopConnection()
	log.Printf("[INFO] StopConnection")

	<-timeout.C
}

