package main

import (
	"context"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
)



var (
	listeners []*Listener
	Addrs []string
	stopChan = make(chan struct{})
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
		listeners = append(listeners, l)
	}

	go startServers(ctx, stopChan, listeners)
	if inheritListeners != nil {
		// ack old process start success
		if _, err := listenSockConn.Write([]byte{0}); err != nil {
			log.Fatalf("[new] process graceful failed, exit")
		}
		listenSockConn.Close()
		// close inheritListener
		for _, ln := range inheritListeners {
			if ln != nil {
				log.Printf("[new] close useless legacy listener: %s", ln.Addr().String())
				ln.Close()
			}
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

func startServers(ctx context.Context, stopChan chan struct{}, ls []*Listener) {
	for _, l := range ls {
		go func(l *Listener) {
			log.Printf("server starting, addr: %v\n", l.Addr())
			for {
				conn, err := l.Accept()
				if err == nil {
					go handler(conn)
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
		}(l)
	}
	// close
	select {
	case <-stopChan:
		for _,l := range ls {
			l.StopAccept()
		}
	case <-ctx.Done():
		for _,l := range ls {
			l.Close()
		}
	}
}

func StopAccept() {
	stopChan <- struct{}{}
}

// echo server
func handler(conn net.Conn) {
	// close connection.go on exit
	defer func() {
		conn.Close()
		log.Printf("[INFO] conn %v close\n", conn.RemoteAddr())
	}()

	var buf [512]byte
	for {
		// read upto 512 bytes
		n, err := conn.Read(buf[0:])
		if err != nil {
			log.Printf("[ERROR] [handler] read form conn error: %v\n", err)
			return
		}
		log.Printf("recv dataLength: %v data: %v", n, string(buf[:n]))
		// write the n bytes read
		_, err2 := conn.Write(append([]byte("server: "), buf[:n]...))
		if err2 != nil {
			log.Printf("[ERROR] [handler] write to conn error: %v\n", err)
			return
		}
	}
}

func listListenerFiles() ([]*os.File, error) {
	fs := make([]*os.File, 0, len(listeners))
	for _, l := range listeners {
		file, err := l.ListenerFile()
		if err != nil {
			return nil, err
		}
		fs = append(fs, file)
	}
	return fs, nil
}
