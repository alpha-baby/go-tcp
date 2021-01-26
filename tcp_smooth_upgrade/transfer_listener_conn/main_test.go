package main

import (
	"net"
	"testing"
	"time"
)

func TestTransfer(t *testing.T) {
	type Case struct {
		name       string
		sendMsg    string
		testperiod time.Duration
		addr       string
	}
	testCase := []Case{
		Case{
			name:       "#1",
			sendMsg:    "testMsg",
			testperiod: time.Minute * 10,
			addr:       "127.0.0.1:1234",
		},
	}
	for _, tt := range testCase {
		deadTime := time.Now().Add(tt.testperiod)
		conn, err := net.Dial("tcp", tt.addr)
		if err != nil {
			t.Errorf("test case: %v error: %v ", tt.name, err)
			return
		}
		defer func(c net.Conn) {
			if c != nil {
				c.Close()
			}
		}(conn)

		t.Run(tt.name, func(t *testing.T) {

			for {
				if deadTime.Before(time.Now()) {
					return
				}

				conn.SetWriteDeadline(time.Now().Add(3*time.Second))
				n, err := conn.Write([]byte(tt.sendMsg))
				if err != nil {
					t.Error(err)
					return
				}
				if n != len(tt.sendMsg) {
					t.Errorf("send msg len: %v, want: %v", n, len(tt.sendMsg))
					return
				}
				buf := make([]byte, 1<<7)

				conn.SetReadDeadline(time.Now().Add(10*time.Second))
				n, err = conn.Read(buf)
				if err != nil {
					t.Error(err)
					return
				}
				if string(buf[:n]) != "server: "+tt.sendMsg {
					t.Errorf("recv msg: %v, want: %v", string(buf[:n]), "server: "+tt.sendMsg)
					return
				}
			}

		})
	}

}
