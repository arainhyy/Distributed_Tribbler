package storageserver

import (
	"errors"
	"container/list"
	"github.com/cmu440/tribbler/rpc/storagerpc"
	"net"
	"net/rpc"
	"net/http"
	"fmt"
	"time"
	"log"
	"os"
)

type storageServer struct {
	nodeID               uint32
	numNodes             int
	nodes                []storagerpc.Node
	nodeIndex            int
	masterServerHostPort string
	port                 int

	addSlave             chan *storagerpc.RegisterArgs
	addSlaveReturn       chan *storagerpc.RegisterReply

	getServers           chan *storagerpc.GetServersArgs
	getServersReturn     chan *storagerpc.GetServersReply

	clients              map[string]string
	tribblers            map[string]*list.List

	put                  chan *storagerpc.PutArgs
	putList              chan *storagerpc.PutArgs

	delete               chan *storagerpc.DeleteArgs
	deleteList           chan *storagerpc.PutArgs

	getRequest           chan *storagerpc.GetArgs
	getListRequest       chan *storagerpc.GetArgs

	putReturn            chan *storagerpc.PutReply
	putListReturn        chan *storagerpc.PutReply

	getReturn            chan *storagerpc.GetReply
	getListReturn        chan *storagerpc.GetListReply

	deleteReturn         chan *storagerpc.DeleteReply
	deleteListReturn     chan *storagerpc.PutReply
}

// NewStorageServer creates and starts a new StorageServer. masterServerHostPort
// is the master storage server's host:port address. If empty, then this server
// is the master; otherwise, this server is a slave. numNodes is the total number of
// servers in the ring. port is the port number that this server should listen on.
// nodeID is a random, unsigned 32-bit ID identifying this server.
//
// This function should return only once all storage servers have joined the ring,
// and should return a non-nil error if the storage server could not be started.
var LOGF *log.Logger

func NewStorageServer(masterServerHostPort string, numNodes, port int, nodeID uint32) (StorageServer, error) {
	newStorageServer := new(storageServer)

	newStorageServer.clients = make(map[string]string)
	newStorageServer.tribblers = make(map[string]*list.List)
	newStorageServer.delete = make(chan *storagerpc.DeleteArgs, 1000)
	newStorageServer.deleteReturn = make(chan *storagerpc.DeleteReply, 1000)
	newStorageServer.deleteList = make(chan *storagerpc.PutArgs, 1000)
	newStorageServer.deleteListReturn = make(chan *storagerpc.PutReply, 1000)

	newStorageServer.getRequest = make(chan *storagerpc.GetArgs, 1000)
	newStorageServer.getListRequest = make(chan *storagerpc.GetArgs, 1000)
	newStorageServer.getReturn = make(chan *storagerpc.GetReply, 1000)
	newStorageServer.getListReturn = make(chan *storagerpc.GetListReply, 1000)

	newStorageServer.put = make(chan *storagerpc.PutArgs, 1000)
	newStorageServer.putList = make(chan *storagerpc.PutArgs, 1000)
	newStorageServer.putReturn = make(chan *storagerpc.PutReply, 1000)
	newStorageServer.putListReturn = make(chan *storagerpc.PutReply, 1000)

	newStorageServer.nodeID = nodeID
	newStorageServer.masterServerHostPort = masterServerHostPort
	newStorageServer.numNodes = numNodes
	newStorageServer.port = port

	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return nil, errors.New("Fail to listen.")
	}
	err = rpc.RegisterName("StorageServer", storagerpc.Wrap(newStorageServer))
	if err != nil {
		return nil, errors.New("Fail to register storageServer.")
	}
	rpc.HandleHTTP()
	go http.Serve(listener, nil)

	initRegister(masterServerHostPort, newStorageServer, numNodes, port, nodeID)

	go storageServerRoutine(newStorageServer)

	return newStorageServer, nil
}

func initRegister(masterServerHostPort string, newStorageServer *storageServer, numNodes, port int, nodeID uint32)  {
	if len(masterServerHostPort) == 0 {
		newStorageServer.nodes = make([]storagerpc.Node, numNodes, numNodes)
		newStorageServer.nodes[0].HostPort = ""
		newStorageServer.nodes[0].NodeID = nodeID
		newStorageServer.nodeIndex = 1

		newStorageServer.addSlave = make(chan *storagerpc.RegisterArgs, 100)
		newStorageServer.addSlaveReturn = make(chan *storagerpc.RegisterReply, 100)
		newStorageServer.getServers = make(chan *storagerpc.GetServersArgs, 100)
		newStorageServer.getServersReturn = make(chan *storagerpc.GetServersReply, 100)

	} else {
		var cli *rpc.Client
		var err error
		for {
			cli, err = rpc.DialHTTP("tcp", masterServerHostPort)
			if err == nil {
				break
			} else {
				time.Sleep(1000 * time.Millisecond)
			}
		}
		for {
			args := &storagerpc.RegisterArgs{ServerInfo:storagerpc.Node{HostPort:fmt.Sprintf(":%d", port), NodeID:nodeID}}
			var reply storagerpc.RegisterReply
			err = cli.Call("StorageServer.RegisterServer", args, &reply)
			if reply.Status == storagerpc.OK {
				break
			} else {
				time.Sleep(1000 * time.Millisecond)
			}
		}
	}

}

func (ss *storageServer) RegisterServer(args *storagerpc.RegisterArgs, reply *storagerpc.RegisterReply) error {
	ss.addSlave <- args
	replyTmp := <-ss.addSlaveReturn
	reply.Status = replyTmp.Status
	reply.Servers = replyTmp.Servers

	return nil
}

func (ss *storageServer) GetServers(args *storagerpc.GetServersArgs, reply *storagerpc.GetServersReply) error {
	ss.getServers <- args
	replyTmp := <-ss.getServersReturn
	reply.Status = replyTmp.Status
	reply.Servers = replyTmp.Servers
	return nil
}

func (ss *storageServer) Get(args *storagerpc.GetArgs, reply *storagerpc.GetReply) error {
	ss.getRequest <- args
	replyTmp := <-ss.getReturn
	reply.Status = replyTmp.Status
	reply.Lease = replyTmp.Lease
	reply.Value = replyTmp.Value
	return nil
}

func (ss *storageServer) Delete(args *storagerpc.DeleteArgs, reply *storagerpc.DeleteReply) error {
	ss.delete <- args
	replyTmp := <-ss.deleteReturn
	reply.Status = replyTmp.Status
	return nil
}

func (ss *storageServer) GetList(args *storagerpc.GetArgs, reply *storagerpc.GetListReply) error {
	ss.getListRequest <- args
	replyTmp := <-ss.getListReturn
	reply.Status = replyTmp.Status
	reply.Value = replyTmp.Value
	reply.Lease = replyTmp.Lease
	return nil
}

func (ss *storageServer) Put(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	ss.put <- args
	replyTmp := <-ss.putReturn
	reply.Status = replyTmp.Status
	return nil
}

func (ss *storageServer) AppendToList(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	ss.putList <- args
	replyTmp := <-ss.putListReturn
	reply.Status = replyTmp.Status

	return nil
}

func (ss *storageServer) RemoveFromList(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	ss.deleteList <- args
	replyTmp := <-ss.deleteListReturn
	reply.Status = replyTmp.Status

	return nil
}

func storageServerRoutine(ss *storageServer) {
	const (
		name = "log.txt"
		flag = os.O_RDWR | os.O_CREATE
		perm = os.FileMode(0666)
	)

	file, _ := os.OpenFile(name, flag, perm)

	defer file.Close()


	LOGF = log.New(file, "", log.Lshortfile|log.Lmicroseconds)
	LOGF.Println("getList")
	for {
		select {
		case addServerRequest := <-ss.addSlave:
			addServerFunc(ss, addServerRequest)

		case getServerRequest := <-ss.getServers:
			getServerFunc(ss, getServerRequest)

		case putRequest := <-ss.put:
			putRequestFunc(ss, putRequest)

		case request := <-ss.putList:
			putListRequestFunc(ss, request)

		case request := <-ss.getRequest:
			getRequestFunc(ss, request)

		case request := <-ss.getListRequest:
			getListRequestFunc(ss, request)

		case deleteRequest := <-ss.delete:
			deleteRequestFunc(ss, deleteRequest)

		case deleteListRequest := <-ss.deleteList:
			deleteListRequestFunc(ss, deleteListRequest)

		}
	}
}

func addServerFunc(ss *storageServer, addServerRequest *storagerpc.RegisterArgs) {
	alreadyRegistered := false
	for i := 0; i < ss.nodeIndex; i++ {
		if ss.nodes[i].NodeID == addServerRequest.ServerInfo.NodeID {
			alreadyRegistered = true
			break
		}
	}
	if !alreadyRegistered {
		ss.nodes[ss.nodeIndex].NodeID = addServerRequest.ServerInfo.NodeID
		ss.nodes[ss.nodeIndex].HostPort = addServerRequest.ServerInfo.HostPort
		ss.nodeIndex++
	}

	re := storagerpc.RegisterReply{}
	re.Servers = ss.nodes
	if ss.nodeIndex >= ss.numNodes {
		re.Status = storagerpc.OK
	} else {
		re.Status = storagerpc.NotReady
	}
	ss.addSlaveReturn <- &re
}

func getServerFunc(ss *storageServer, getServerRequest *storagerpc.GetServersArgs) { // getServerRequest is empty
	re := storagerpc.GetServersReply{}
	re.Servers = ss.nodes
	if ss.nodeIndex == ss.numNodes {
		re.Status = storagerpc.OK
	} else {
		re.Status = storagerpc.NotReady
	}
	ss.getServersReturn <- &re
}

func putRequestFunc(ss *storageServer, putRequest *storagerpc.PutArgs) {
	ss.clients[putRequest.Key] = putRequest.Value
	re := storagerpc.PutReply{Status: storagerpc.OK}

	/*if _, ok := ss.tribblers[putRequest.Key]; !ok {
		ss.tribblers[putRequest.Key] = list.New()
	}*/
	ss.putReturn <- &re
}

func putListRequestFunc(ss *storageServer, request *storagerpc.PutArgs) {
	LOGF.Println("append to list ", request.Value)
	if _, ok := ss.tribblers[request.Key]; !ok {
		/*re := storagerpc.PutReply{Status: storagerpc.KeyNotFound}
		ss.putListReturn <- &re
		return*/
		ss.tribblers[request.Key] = list.New()
	}
	flag := false
	for e := ss.tribblers[request.Key].Front(); e != nil; e = e.Next() {
		if e.Value == request.Value {
			flag = true
			break
		}
	}
	if flag {
		re := storagerpc.PutReply{Status: storagerpc.ItemExists}
		ss.putListReturn <- &re
		return
	}
	ss.tribblers[request.Key].PushFront(request.Value)
	re := storagerpc.PutReply{Status: storagerpc.OK}
	ss.putListReturn <- &re
}

func getRequestFunc(ss *storageServer, request *storagerpc.GetArgs) {
	if _, ok := ss.clients[request.Key]; !ok {
		re := storagerpc.GetReply{Status: storagerpc.KeyNotFound}
		ss.getReturn <- &re
		return
	}
	re := storagerpc.GetReply{Status: storagerpc.OK}
	re.Value = ss.clients[request.Key]
	ss.getReturn <- &re
}

func getListRequestFunc(ss *storageServer, request *storagerpc.GetArgs) {
	LOGF.Println("getList")
	if _, ok := ss.tribblers[request.Key]; !ok {
		re := storagerpc.GetListReply{Status: storagerpc.KeyNotFound}
		ss.getListReturn <- &re
		return
	}
	re := storagerpc.GetListReply{Status: storagerpc.OK}

	var str []string

	for e := ss.tribblers[request.Key].Front(); e != nil; e = e.Next() {
		str = append(str, e.Value.(string))
	}
	re.Value = str
	ss.getListReturn <- &re
}

func deleteRequestFunc(ss *storageServer, deleteRequest *storagerpc.DeleteArgs) {
	if _, ok := ss.clients[deleteRequest.Key]; !ok {
		re := storagerpc.DeleteReply{Status: storagerpc.KeyNotFound}
		ss.deleteReturn <- &re
		return
	}
	delete(ss.clients, deleteRequest.Key)
	delete(ss.tribblers, deleteRequest.Key)
	re := storagerpc.DeleteReply{Status: storagerpc.OK}
	ss.deleteReturn <- &re
}

func deleteListRequestFunc(ss *storageServer, deleteListRequest *storagerpc.PutArgs) {
	if _, ok := ss.tribblers[deleteListRequest.Key]; !ok {
		re := storagerpc.PutReply{Status: storagerpc.KeyNotFound}
		ss.deleteListReturn <- &re
		return
	}
	flag := false
	for e := ss.tribblers[deleteListRequest.Key].Front(); e != nil; e = e.Next() {
		if e.Value == deleteListRequest.Value {
			flag = true
			ss.tribblers[deleteListRequest.Key].Remove(e)
			break
		}
	}
	if !flag {
		re := storagerpc.PutReply{Status: storagerpc.ItemNotFound}
		ss.deleteListReturn <- &re
		return
	} else {
		re := storagerpc.PutReply{Status: storagerpc.OK}
		ss.deleteListReturn <- &re
	}
}
