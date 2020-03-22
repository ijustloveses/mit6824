package main

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
	"time"
)

type EchoService struct {
	shutdown chan string
	l        net.Listener
	address  *net.TCPAddr
}

func (service *EchoService) Shutdown(_, _ *struct{}) error {
	fmt.Println("Shutdown got called, close shutdown channel")
	close(service.shutdown)
	fmt.Println("close listener")
	service.l.Close() // causes the Accept to fail
	return nil
}

func (service *EchoService) stopRPCServer() {
	var reply struct{}
	ok := call("127.0.0.1:1234", "EchoService.Shutdown", new(struct{}), &reply)
	if ok == false {
		fmt.Printf("Cleanup: RPC %s error\n", service.address)
	}
}

func (service *EchoService) RegisterAndServeOnTcp() {
	err := rpc.Register(service)
	if err != nil {
		log.Fatal("Register failed", err)
		return
	}
	tcpAddr, err := net.ResolveTCPAddr("tcp", ":1234")
	if err != nil {
		log.Fatal("Resolving tcp failed", err)
		return
	}
	service.address = tcpAddr
	listener, err := net.ListenTCP("tcp", tcpAddr)

	if err != nil {
		log.Fatal("Listening failed", err)
		return
	}
	service.l = listener

	go func() {
	loop:
		for {
			fmt.Println("Start to select")
			select {
			case <-service.shutdown:
				fmt.Println("recv from shutdown channel")
				break loop
			default:
			}

			fmt.Println("go to Accept")
			conn, err := service.l.Accept()
			if err != nil {
				log.Fatal("accept failed", err)
			} else {
				fmt.Println("get accepted")
				go func() {
					rpc.ServeConn(conn)
					conn.Close()
				}()
			}
		}
	}()
}

func call(srv string, rpcname string,
	args interface{}, reply interface{}) bool {
	c, errx := rpc.Dial("tcp", srv)
	if errx != nil {
		return false
	}
	defer c.Close()

	err := c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

func main() {
	service := new(EchoService)
	service.shutdown = make(chan string)
	service.RegisterAndServeOnTcp()
	time.Sleep(1e9)
	service.shutdown <- "shutdown now"
	time.Sleep(2e10)
	fmt.Println("wait up and call shutdown!")
	service.stopRPCServer()
}
