package storageserver

import (
	"errors"
	"container/list"
	"github.com/cmu440/tribbler/rpc/storagerpc"
	"net"
	"net/rpc"
	"net/http"
	"strconv"
)

type storageServer struct {
	nodeID               uint32
	masterServerHostPort string
	numNodes             int

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
func NewStorageServer(masterServerHostPort string, numNodes, port int, nodeID uint32) (StorageServer, error) {
	newStorageServer := storageServer{}

	newStorageServer.clients = make(map[string]string)
	newStorageServer.tribblers = make(map[string]*list.List)
	newStorageServer.delete = make(chan *storagerpc.DeleteArgs, 1000)
	newStorageServer.deleteReturn = make(chan *storagerpc.DeleteReply, 1000)

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

	listener, err := net.Listen("tcp", "localhost:" + strconv.Itoa(port))
	err = rpc.RegisterName("StorageServer", storagerpc.Wrap(&newStorageServer))
	if err != nil {
		return nil, errors.New("Fail to register storageServer.")
	}
	rpc.HandleHTTP()
	go http.Serve(listener, nil)

	go storageServerRoutine(&newStorageServer)
	return &newStorageServer, nil
}

func (ss *storageServer) RegisterServer(args *storagerpc.RegisterArgs, reply *storagerpc.RegisterReply) error {
	return errors.New("not implemented")
}

func (ss *storageServer) GetServers(args *storagerpc.GetServersArgs, reply *storagerpc.GetServersReply) error {
	return errors.New("not implemented")
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
	for {
		select {
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

func putRequestFunc(ss *storageServer, putRequest *storagerpc.PutArgs) {
	ss.clients[putRequest.Key] = putRequest.Value
	re := storagerpc.PutReply{Status: storagerpc.OK}

	if _, ok := ss.tribblers[putRequest.Key]; !ok {
		ss.tribblers[putRequest.Key] = list.New()
	}
	ss.putReturn <- &re
}

func putListRequestFunc(ss *storageServer, request *storagerpc.PutArgs) {
	if _, ok := ss.tribblers[request.Key]; !ok {
		re := storagerpc.PutReply{Status: storagerpc.KeyNotFound}
		ss.putReturn <- &re
		return
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
		ss.putReturn <- &re
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
		ss.putReturn <- &re
		return
	}
	flag := false
	for e := ss.tribblers[deleteListRequest.Key].Front(); e != nil; e = e.Next() {
		if e.Value.(string) == deleteListRequest.Value {
			flag = true
			ss.tribblers[deleteListRequest.Key].Remove(e)
			break
		}
	}
	if !flag {
		re := storagerpc.PutReply{Status: storagerpc.ItemNotFound}
		ss.putReturn <- &re
		return
	} else {
		re := storagerpc.PutReply{Status: storagerpc.OK}
		ss.putReturn <- &re
	}
}
