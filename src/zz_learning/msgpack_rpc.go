package main

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/vmihailenco/msgpack/v4"
)

/*
go rpc 提供了两个接口：ServerCodec & ClientCodec
type ServerCodec interface {
	ReadRequestHeader(*Request) error
	ReadRequestBody(interface{}) error
	WriteResponse(*Response, interface{}) error
	Close() error
}
type ClientCodec interface {
	WriteRequest(*Request, interface{}) error
	ReadResponseHeader(*Response) error
	ReadResponseBody(interface{}) error
	Close() error
}

go rpc将一次请求/响应抽象成了header+body的形式：
- 读取数据时分为读取head和读取body，
- 写入数据时只需写入body部分，go rpc会替我们加上head部分

故此，下面我们来实现一次请求/响应的完整数据，内嵌了go rpc里自带的Request和Response
自带的Request和Response定义了序号、方法名等信息，这里不加调整
可以看到，下面定义的 MsgpackReq & MsgpackResp 的字段都是从上面 ServerCodec & ClientCodec 中来的
*/
type MsgpackReq struct {
	rpc.Request             // header
	Arg         interface{} // body
}

type MsgpackResp struct {
	rpc.Response             // header
	Reply        interface{} // body
}

/*
接着就是自定义Codec的声明，会实现 go rpc 要求的接口：ServerCodec & ClientCodec
*/
type MsgpackServerCodec struct {
	rwc    io.ReadWriteCloser // 用于读写数据，实际是一个网络连接
	req    MsgpackReq         // 用于缓存解析到的请求，Server 解析 Request
	closed bool               // 标识codec是否关闭
}

type MsgpackClientCodec struct {
	rwc    io.ReadWriteCloser // 用于读写数据，实际是一个网络连接
	resp   MsgpackResp        // 用于缓存解析到的请求，Client 解析 Response
	closed bool               // 标识codec是否关闭
}

func NewServerCodec(conn net.Conn) *MsgpackServerCodec {
	return &MsgpackServerCodec{conn, MsgpackReq{}, false}
}

func NewClientCodec(conn net.Conn) *MsgpackClientCodec {
	return &MsgpackClientCodec{conn, MsgpackResp{}, false}
}

/*
出于简单考虑，将反序列化部分的两步合并为一步，在读取head部分时就将所有的数据解析好并缓存起来，读取body时直接返回缓存的结果。
这个用于缓存的数据结构就是上面定义的 MsgpackReq & MsgpackResp。具体的，整个流程如下：
1. 客户端在发送请求时，将数据包装成一个MsgpackReq，然后用messagepack序列化并发送出去
2. 服务端在读取请求head部分时，将收到的数据用messagepack反序列化成一个MsgpackReq，并将得到的结果缓存起来
3. 服务端在读取请求body部分时，从缓存的MsgpackReq中获取到Arg字段并返回
4. 服务端在发送响应时，将数据包装成一个MsgpackResp，然后用messagepack序列化并发送出去
5. 客户端在读取响应head部分时，将收到的数据用messagepack反序列化成一个MsgpackResp，并将得到的结果缓存起来
6. 客户端在读取响应body部分时，从缓存的MsgpackResp中获取到Reply或者Error字段并返回
*/

/* step 1. 客户端打包消息
   函数格式为 go rpc ClientCodec 所定义，以下均是如此 */
func (c *MsgpackClientCodec) WriteRequest(r *rpc.Request, arg interface{}) error {
	if c.closed {
		return nil
	}
	request := &MsgpackReq{*r, arg}          // 把参数整理为 MsgpackReq 形式
	reqData, err := msgpack.Marshal(request) // 把 MsgpackReq 打包
	if err != nil {
		panic(err)
		return err
	}

	head := make([]byte, 4)
	binary.BigEndian.PutUint32(head, uint32(len(reqData))) // head 保存请求的 byte 数
	_, err = c.rwc.Write(head)                             // 发送 head
	_, err = c.rwc.Write(reqData)                          // 发送打包后的消息体
	return err
}

/* step 5. 客户端读取 response 包后解包并获取 header
   在具体的接口实现之前，首先实现一个从 conn 读取 head & 消息体 body 的函数
*/
func readData(conn io.ReadWriteCloser) (data []byte, returnError error) {
	const HeadSize = 4 // 前面看到，header 部分长度设置为 4
	headBuf := bytes.NewBuffer(make([]byte, 0, HeadSize))
	headData := make([]byte, HeadSize)

	// 从 conn 中读取 header 部分
	for {
		readSize, err := conn.Read(headData) // 读取长度不超过 headData 长度（初始为 4）的数据
		if err != nil {
			returnError = err
			return
		}
		headBuf.Write(headData[0:readSize]) // 一旦读到数据，就写入 buffer，headBuf.Len() 会增加
		if headBuf.Len() == HeadSize {      // 如果确实读取到 4 个长度，那么就完成 Header 部分了
			break
		} else { // 否则，已经读了 3 个长度（headBuf 长度现在为 3），那么把 headData 长度缩减为 1，继续读取
			headData = make([]byte, HeadSize-headBuf.Len())
		}
	}
	// 有了 header 部分，就可以知道 body 部分的长度了
	// 进而类似 header 部分的方式读取 body 部分
	bodyLen := int(binary.BigEndian.Uint32(headBuf.Bytes()))
	bodyBuf := bytes.NewBuffer(make([]byte, 0, bodyLen))
	bodyData := make([]byte, bodyLen)
	for {
		readSize, err := conn.Read(bodyData)
		if err != nil {
			returnError = err
			return
		}
		bodyBuf.Write(bodyData[0:readSize])
		if bodyBuf.Len() == bodyLen {
			break
		} else {
			bodyData = make([]byte, bodyLen-bodyBuf.Len())
		}
	}
	// 整理返回数据结果
	data = bodyBuf.Bytes()
	returnError = nil
	return
}

func (c *MsgpackClientCodec) ReadResponseHeader(r *rpc.Response) error {
	if c.closed {
		return nil
	}
	data, err := readData(c.rwc) // 从 conn 中读取数据，此时数据就是二进制的 MsgpackResp
	if err != nil {
		// client一旦初始化就会开始轮询数据，所以要处理连接close的情况
		if strings.Contains(err.Error(), "use of closed network connection") {
			return nil
		}
		panic(err) // 如果是其他错误，直接 panic
	}

	// 解包为 MsgpackResp
	var response MsgpackResp
	err = msgpack.Unmarshal(data, &response)
	if err != nil {
		panic(err)
	}

	// 根据读取的数据设置 response 的各个属性
	r.ServiceMethod = response.ServiceMethod
	r.Seq = response.Seq
	// 同时缓存结果
	c.resp = response
	return nil
}

/* step 6. 客户端从缓存中获取 body */
func (c *MsgpackClientCodec) ReadResponseBody(reply interface{}) error {
	// 直接使用缓存的数据即可
	if "" != c.resp.Error {
		return errors.New(c.resp.Error)
	}
	if reply != nil {
		// 正常返回，通过反射将结果设置到 reply 变量，而 reply 变量一定是指针类型，不必检查 CanSet
		reflect.ValueOf(reply).Elem().Set(reflect.ValueOf(c.resp.Reply))
	}
	return nil
}

/* 关闭接口 */
func (c *MsgpackClientCodec) Close() error {
	c.closed = true
	if c.rwc != nil {
		return c.rwc.Close()
	}
	return nil
}

/* step 4. 服务端打包响应消息
   函数格式为 go rpc ServiceCodec 所定义，以下均是如此 */
func (c *MsgpackServerCodec) WriteResponse(r *rpc.Response, reply interface{}) error {
	if c.closed {
		return nil
	}
	response := &MsgpackResp{*r, reply}        // 把参数整理为 MsgpackResp 形式
	respData, err := msgpack.Marshal(response) // 把 MsgpackReq 打包
	if err != nil {
		panic(err)
		return err
	}

	head := make([]byte, 4)
	binary.BigEndian.PutUint32(head, uint32(len(respData))) // head 保存请求的 byte 数
	_, err = c.rwc.Write(head)                              // 发送 head
	_, err = c.rwc.Write(respData)                          // 发送打包后的消息体
	return err
}

/* step 2. 服务端读取 request 包后解包并获取 header */
func (c *MsgpackServerCodec) ReadRequestHeader(r *rpc.Request) error {
	if c.closed {
		return nil
	}
	data, err := readData(c.rwc) // 从 conn 中读取数据，此时数据就是二进制的 MsgpackReq
	if err != nil {
		// 这里不能直接panic，需要处理EOF和reset的情况
		if err == io.EOF {
			return err
		}
		if strings.Contains(err.Error(), "connection reset by peer") {
			return err
		}
		panic(err) // 如果是其他错误，直接 panic
	}

	// 解包为 MsgpackResp
	var request MsgpackReq
	err = msgpack.Unmarshal(data, &request)
	if err != nil {
		panic(err)
	}

	// 根据读取的数据设置 request 的各个属性
	r.ServiceMethod = request.ServiceMethod
	r.Seq = request.Seq
	// 同时缓存结果
	c.req = request
	return nil
}

/* step 3. 服务端从缓存中获取 body */
func (c *MsgpackServerCodec) ReadRequestBody(arg interface{}) error {
	// 直接使用缓存的数据即可
	if arg != nil {
		// 正常返回，通过反射将结果设置到 arg 变量，而 arg 变量一定是指针类型，不必检查 CanSet
		reflect.ValueOf(arg).Elem().Set(reflect.ValueOf(c.req.Arg))
	}
	return nil
}

/* 关闭接口 */
func (c *MsgpackServerCodec) Close() error {
	c.closed = true
	if c.rwc != nil {
		return c.rwc.Close()
	}
	return nil
}

/*
Msgpack Server & Client Codec 测试代码
*/

type EchoService struct{}

func (service EchoService) Echo(arg string, result *string) error {
	*result = arg
	return nil
}

func RegisterAndServeOnTcp() {
	err := rpc.Register(&EchoService{}) //注册并不是注册方法，而是注册EchoService的一个实例，用于响应请求
	if err != nil {
		log.Fatal("Register failed", err)
		return
	}
	tcpAddr, err := net.ResolveTCPAddr("tcp", ":1234")
	if err != nil {
		log.Fatal("Resolving tcp failed", err)
		return
	}
	listener, err := net.ListenTCP("tcp", tcpAddr)
	if err != nil {
		log.Fatal("Listening failed", err)
		return
	}

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Fatal("accept failed", err)
		} else {
			// 关键点：先通过NewServerCodec获得一个实例，然后调用rpc.ServeCodec来启动服务
			rpc.ServeCodec(NewServerCodec(conn))
		}
	}
}

func CallEcho(arg string) (result string, err error) {
	var client *rpc.Client
	conn, err := net.Dial("tcp", ":1234")
	// 关键点：对客户端设置 ClientCodec 实例
	client = rpc.NewClientWithCodec(NewClientCodec(conn))
	defer client.Close()

	if err != nil {
		return "", err
	}
	err = client.Call("EchoService.Echo", arg, &result) // 通过类型加方法名指定要调用的方法
	if err != nil {
		return "", err
	}
	return result, err
}

/*
实现的逻辑当中完全没有考虑并发的问题，缓存数据也是直接放到codec对象。
这样简单的实现也不会导致并发调用失败，其中具体的原因就是go rpc在处理每个codec对象时，
读取请求都是顺序的，然后再并发的处理请求并返回结果
*/
func main() {
	go RegisterAndServeOnTcp() // 先启动服务端
	time.Sleep(1e9)            // sleep 1s，因为服务端启动是异步的，所以等一等
	wg := new(sync.WaitGroup)
	callTimes := 10
	wg.Add(callTimes)

	for i := 0; i < callTimes; i++ {
		go func() { // 启动客户端
			arg := "hello world" + strconv.Itoa(rand.Int())
			result, err := CallEcho(arg)
			if err != nil {
				log.Fatal("Calling failed", err)
			}
			if result != arg {
				fmt.Println("Error when matching")
			} else {
				fmt.Println("call echo:", result)
			}
			wg.Done()
		}()
	}
	wg.Wait()
}
