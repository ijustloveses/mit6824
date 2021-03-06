package main

import (
	"fmt"
	"log"
	"net/http"
	"net/rpc"
	"time"
)

/*
go 提供了自带的序列化协议gob（go binary），可以进行原生go类型的序列化和反序列化
一个应用就是go语言自带的rpc功能，主要在net/rpc包下。

go 自带的rpc提供了简单的rpc相关的api，用户只需要依照约定实现function然后进行服务注册，就可以在客户端进行调用了。约定或者约束为：
1. the method's type is exported. 方法所属的类型必须是外部可见的
2. the method has two arguments, both exported (or builtin) types. 方法参数只能有两个，而且必须是外部可见的类型或者是基本类型
3. the method's second argument is a pointer. 方法的第二个参数类型必须是指针，其实是 rpc 调用真正的返回值
4. the method has return type error.方法的返回值必须是error类型
*/

type EchoService struct{} // 满足 1.

func (service EchoService) Echo(arg string, result *string) error { // 参数满足 2、3
	*result = arg
	return nil // 返回值满足 4
}

func RegisterAndServe() {
	err := rpc.Register(&EchoService{}) //注册并不是注册方法，而是注册EchoService的一个实例，用于响应请求
	if err != nil {
		log.Fatal("Register failed", err)
	}
	rpc.HandleHTTP() // rpc通信协议设置为http协议
	err = http.ListenAndServe(":1234", nil)
	if err != nil {
		log.Fatal("Listening failed", err)
	}
}

func CallEcho(arg string) (result string, err error) {
	var client = &rpc.Client{}
	client, err = rpc.DialHTTP("tcp", ":1234") // 通过 rpc.DialHTTP 创建一个 client
	if err != nil {
		return "", err
	}
	err = client.Call("EchoService.Echo", arg, &result) // 通过类型加方法名指定要调用的方法
	if err != nil {
		return "", err
	}
	return result, err
}

func main() {
	done := make(chan int)
	go RegisterAndServe() // 先启动服务端
	time.Sleep(1e9)       // sleep 1s，因为服务端启动是异步的，所以等一等

	go func() { // 启动客户端
		result, err := CallEcho("hello world")
		if err != nil {
			log.Fatal("Calling failed", err)
		} else {
			fmt.Println("call echo:", result)
		}
		done <- 1
	}()

	<-done // 阻塞等待客户端结束
}
