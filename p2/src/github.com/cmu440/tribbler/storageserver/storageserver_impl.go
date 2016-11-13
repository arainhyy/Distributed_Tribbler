package storageserver

import (
	"container/list"
	"errors"
	"fmt"
	"github.com/cmu440/tribbler/rpc/storagerpc"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"time"
	"sync"
	"github.com/cmu440/tribbler/libstore"
)

type hasLeaseClient struct {
	leaseTime time.Time
	hostport  string
}

type storageServer struct {
	nodeID               uint32
	upperBound	uint32
	numNodes             int
	nodes                []storagerpc.Node
	nodeIndex            int
	masterServerHostPort string
	port                 int

	clients              map[string]string
	tribblers            map[string]*list.List

	lock                 *sync.Mutex
	cacheLocks           map[string]*sync.Mutex
	keyClientLeaseMap    map[string][]hasLeaseClient
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
	newStorageServer.cacheLocks = make(map[string]*sync.Mutex)
	newStorageServer.keyClientLeaseMap = make(map[string][]hasLeaseClient)
	newStorageServer.clients = make(map[string]string)
	newStorageServer.tribblers = make(map[string]*list.List)


	newStorageServer.nodeID = nodeID
	newStorageServer.upperBound = 4294967295
	newStorageServer.masterServerHostPort = masterServerHostPort
	newStorageServer.numNodes = numNodes
	newStorageServer.port = port

	newStorageServer.lock = &sync.Mutex{}

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

func initRegister(masterServerHostPort string, newStorageServer *storageServer, numNodes, port int, nodeID uint32) {
	newStorageServer.nodes = make([]storagerpc.Node, numNodes, numNodes)
	if len(masterServerHostPort) == 0 {
		newStorageServer.nodes[0].HostPort = fmt.Sprintf(":%d", port)
		newStorageServer.nodes[0].NodeID = nodeID
		newStorageServer.nodeIndex = 1

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
			args := &storagerpc.RegisterArgs{ServerInfo: storagerpc.Node{HostPort: fmt.Sprintf(":%d", port), NodeID: nodeID}}
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
	replyTmp := addServerFunc(ss, args)
	reply.Status = replyTmp.Status
	reply.Servers = replyTmp.Servers

	return nil
}

func (ss *storageServer) GetServers(args *storagerpc.GetServersArgs, reply *storagerpc.GetServersReply) error {
	replyTmp := getServerFunc(ss, args)
	reply.Status = replyTmp.Status
	ss.nodes = replyTmp.Servers
	reply.Servers = replyTmp.Servers
	return nil
}

func (ss *storageServer) Get(args *storagerpc.GetArgs, reply *storagerpc.GetReply) error {
	replyTmp := getRequestFunc(ss, args)
	reply.Status = replyTmp.Status
	reply.Lease = replyTmp.Lease
	reply.Value = replyTmp.Value
	return nil
}

func (ss *storageServer) Delete(args *storagerpc.DeleteArgs, reply *storagerpc.DeleteReply) error {
	replyTmp := deleteRequestFunc(ss, args)
	reply.Status = replyTmp.Status
	return nil
}

func (ss *storageServer) GetList(args *storagerpc.GetArgs, reply *storagerpc.GetListReply) error {
	replyTmp := getListRequestFunc(ss, args)
	reply.Status = replyTmp.Status
	reply.Value = replyTmp.Value
	reply.Lease = replyTmp.Lease
	return nil
}

func (ss *storageServer) Put(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	replyTmp := putRequestFunc(ss, args)
	reply.Status = replyTmp.Status
	return nil
}

func (ss *storageServer) AppendToList(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	replyTmp := putListRequestFunc(ss, args)
	reply.Status = replyTmp.Status

	return nil
}

func (ss *storageServer) RemoveFromList(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	replyTmp := deleteListRequestFunc(ss, args)
	reply.Status = replyTmp.Status

	return nil
}

func storageServerRoutine(ss *storageServer) {
	if len(ss.masterServerHostPort) == 0 {
		const (
			name = "log.txt"
			flag = os.O_RDWR | os.O_CREATE
			perm = os.FileMode(0666)
		)

		file, _ := os.OpenFile(name, flag, perm)

		defer file.Close()

		LOGF = log.New(file, "", log.Lshortfile | log.Lmicroseconds)
		LOGF.Printf("NewStorageServer")
		LOGF.Printf("numNodes : %d", ss.numNodes)
	}
	for {
		select {

		}
	}
}

func addServerFunc(ss *storageServer, addServerRequest *storagerpc.RegisterArgs) *storagerpc.RegisterReply{

	LOGF.Print("add Server")
	ss.lock.Lock()
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
		LOGF.Printf("register one ")
	}

	re := storagerpc.RegisterReply{}
	re.Servers = ss.nodes
	if ss.nodeIndex >= ss.numNodes {
		re.Status = storagerpc.OK
	} else {
		re.Status = storagerpc.NotReady
	}
	min := uint32(4294967295)
	for i := 0; i < len(ss.nodes); i++ {
		LOGF.Printf("server %d : nodeID: %d, hostport : %s", i, ss.nodes[i].NodeID, ss.nodes[i].HostPort)
		if ss.nodes[i].NodeID < ss.upperBound &&
			ss.nodes[i].NodeID > ss.nodeID {
			ss.upperBound = ss.nodes[i].NodeID - 1
		}
		if ss.nodes[i].NodeID < min {
			min = ss.nodes[i].NodeID
		}
	}
	if ss.upperBound == 4294967295 {
		ss.upperBound = min - 1
	}
	ss.lock.Unlock()
	LOGF.Printf("add Server finish")
	return &re
}

func getServerFunc(ss *storageServer, getServerRequest *storagerpc.GetServersArgs) *storagerpc.GetServersReply{

	LOGF.Printf("get Server")
	ss.lock.Lock()
	// getServerRequest is empty
	re := storagerpc.GetServersReply{}
	re.Servers = ss.nodes
	if ss.nodeIndex == ss.numNodes {
		re.Status = storagerpc.OK

	} else {
		re.Status = storagerpc.NotReady
	}
	ss.lock.Unlock()
	LOGF.Printf("get Server finish")
	return &re
}

func putRequestFunc(ss *storageServer, putRequest *storagerpc.PutArgs) *storagerpc.PutReply{
	LOGF.Printf("put, key is %s", putRequest.Key)
	ss.lock.Lock()

	hashVal := libstore.StoreHash(putRequest.Key)
	if (ss.upperBound < ss.nodeID && (hashVal < ss.nodeID || hashVal > ss.upperBound)) ||
		(hashVal > ss.upperBound && hashVal < ss.nodeID){
		LOGF.Printf("WrongServer, lowerBound: %d, key: %d, upperBound: %d", ss.nodeID, hashVal, ss.upperBound)
		re := storagerpc.PutReply{Status: storagerpc.WrongServer}
		ss.lock.Unlock()
		return &re
	}

	if _, ok := ss.cacheLocks[putRequest.Key]; !ok {
		// it's a new key, init the mutex for this key
		ss.cacheLocks[putRequest.Key] = &sync.Mutex{}
		ss.keyClientLeaseMap[putRequest.Key] = make([]hasLeaseClient, 0)
	}

	ss.cacheLocks[putRequest.Key].Lock()
	ss.lock.Unlock()
	// look for all clients that have been granted the lease
	// lock to grant new leases for the key
	// launch new goroutine to send revokeLease rpc to all these clients
	// after all rpc received the OK, unlock to grant new leases
	re := storagerpc.PutReply{Status: storagerpc.OK}

	LOGF.Printf("lock already exist, begin revoke")
	sendRevokeLease(ss, putRequest.Key)

	ss.clients[putRequest.Key] = putRequest.Value
	ss.cacheLocks[putRequest.Key].Unlock()
	return &re

}

func putListRequestFunc(ss *storageServer, request *storagerpc.PutArgs) *storagerpc.PutReply{
	LOGF.Println("append to list ", request.Value)
	ss.lock.Lock()

	hashVal := libstore.StoreHash(request.Key)
	if (ss.upperBound < ss.nodeID && (hashVal < ss.nodeID || hashVal > ss.upperBound)) ||
		(hashVal > ss.upperBound && hashVal < ss.nodeID){
		re := storagerpc.PutReply{Status: storagerpc.WrongServer}
		ss.lock.Unlock()
		LOGF.Println("append to list finish 1")
		return &re
	}

	if _, ok := ss.tribblers[request.Key]; !ok {
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
		ss.lock.Unlock()
		LOGF.Println("append to list finish 2")
		return &re
	}

	if _, ok := ss.cacheLocks[request.Key]; ok {
		// it's a new key, init the mutex for this key
		ss.cacheLocks[request.Key] = &sync.Mutex{}
		ss.keyClientLeaseMap[request.Key] = make([]hasLeaseClient, 0)
	}
	ss.cacheLocks[request.Key].Lock()
	ss.lock.Unlock()

	sendRevokeLease(ss, request.Key)
	ss.tribblers[request.Key].PushFront(request.Value)

	re := storagerpc.PutReply{Status: storagerpc.OK}

	LOGF.Println("append to list finish 3")
	ss.cacheLocks[request.Key].Unlock()
	return &re

}

func getRequestFunc(ss *storageServer, request *storagerpc.GetArgs) *storagerpc.GetReply{
	LOGF.Printf("get, key is %s", request.Key)
	ss.lock.Lock()
	hashVal := libstore.StoreHash(request.Key)
	if (ss.upperBound < ss.nodeID && (hashVal < ss.nodeID || hashVal > ss.upperBound)) ||
		(hashVal > ss.upperBound && hashVal < ss.nodeID){
		re := storagerpc.GetReply{Status: storagerpc.WrongServer}
		ss.lock.Unlock()
		LOGF.Printf("get finish 1")
		return &re
	}

	if _, ok := ss.clients[request.Key]; !ok {
		re := storagerpc.GetReply{Status: storagerpc.KeyNotFound}
		ss.lock.Unlock()
		LOGF.Printf("get finish 2")
		return &re
	}
	re := storagerpc.GetReply{Status: storagerpc.OK}
	re.Value = ss.clients[request.Key]

	if request.WantLease {
		LOGF.Printf("want lease, key : %s", request.Key)
		ss.cacheLocks[request.Key].Lock() // wait for lease can be granted
		tmp := append(ss.keyClientLeaseMap[request.Key], hasLeaseClient{leaseTime: time.Now(), hostport: request.HostPort})
		ss.keyClientLeaseMap[request.Key] = tmp
		LOGF.Printf("lease size : %d", len(ss.keyClientLeaseMap[request.Key]))
		re.Lease.Granted = true
		re.Lease.ValidSeconds = storagerpc.LeaseSeconds
		ss.cacheLocks[request.Key].Unlock()
	}
	ss.lock.Unlock()
	LOGF.Printf("get finish 3")
	return &re
}

func getListRequestFunc(ss *storageServer, request *storagerpc.GetArgs) *storagerpc.GetListReply{

	LOGF.Printf("get, key is %s", request.Key)
	ss.lock.Lock()
	hashVal := libstore.StoreHash(request.Key)
	if (ss.upperBound < ss.nodeID && (hashVal < ss.nodeID || hashVal > ss.upperBound)) ||
		(hashVal > ss.upperBound && hashVal < ss.nodeID){
		re := storagerpc.GetListReply{Status: storagerpc.WrongServer}
		ss.lock.Unlock()
		LOGF.Printf("get list finish 1")
		return &re
	}

	if _, ok := ss.tribblers[request.Key]; !ok {
		re := storagerpc.GetListReply{Status: storagerpc.KeyNotFound}
		ss.lock.Unlock()
		LOGF.Printf("get list finish 2")
		return &re
	}
	re := storagerpc.GetListReply{Status: storagerpc.OK}

	var str []string

	for e := ss.tribblers[request.Key].Front(); e != nil; e = e.Next() {
		str = append(str, e.Value.(string))
	}
	re.Value = str

	if request.WantLease {
		LOGF.Printf("want lease, key : %s", request.Key)
		ss.cacheLocks[request.Key].Lock() // wait for lease can be granted
		tmp := append(ss.keyClientLeaseMap[request.Key], hasLeaseClient{leaseTime: time.Now(), hostport: request.HostPort})
		ss.keyClientLeaseMap[request.Key] = tmp
		re.Lease.Granted = true
		re.Lease.ValidSeconds = storagerpc.LeaseSeconds
		ss.cacheLocks[request.Key].Unlock()
	}
	ss.lock.Unlock()

	LOGF.Printf("get list finish 3")
	return &re
}

func deleteRequestFunc(ss *storageServer, deleteRequest *storagerpc.DeleteArgs) *storagerpc.DeleteReply{
	LOGF.Printf("delete: %s", deleteRequest.Key)
	ss.lock.Lock()

	hashVal := libstore.StoreHash(deleteRequest.Key)
	if (ss.upperBound < ss.nodeID && (hashVal < ss.nodeID || hashVal > ss.upperBound)) ||
		(hashVal > ss.upperBound && hashVal < ss.nodeID){
		re := storagerpc.DeleteReply{Status: storagerpc.WrongServer}
		ss.lock.Unlock()
		LOGF.Printf("delete finish 1")
		return &re
	}

	if _, ok := ss.clients[deleteRequest.Key]; !ok {
		re := storagerpc.DeleteReply{Status: storagerpc.KeyNotFound}
		ss.lock.Unlock()
		LOGF.Printf("delete finish 2")
		return &re
	}

	ss.cacheLocks[deleteRequest.Key].Lock()
	ss.lock.Unlock()
	sendRevokeLease(ss, deleteRequest.Key)
	delete(ss.clients, deleteRequest.Key)
	//delete(ss.tribblers, deleteRequest.Key)
	re := storagerpc.DeleteReply{Status: storagerpc.OK}
	ss.cacheLocks[deleteRequest.Key].Unlock()

	LOGF.Printf("delete finish 3")
	return &re
}

func deleteListRequestFunc(ss *storageServer, deleteListRequest *storagerpc.PutArgs) *storagerpc.PutReply{
	LOGF.Printf("delete: %s", deleteListRequest.Key)
	ss.lock.Lock()

	hashVal := libstore.StoreHash(deleteListRequest.Key)
	if (ss.upperBound < ss.nodeID && (hashVal < ss.nodeID || hashVal > ss.upperBound)) ||
		(hashVal > ss.upperBound && hashVal < ss.nodeID){
		re := storagerpc.PutReply{Status: storagerpc.WrongServer}
		ss.lock.Unlock()
		LOGF.Printf("delete list finish 1")
		return &re
	}

	if _, ok := ss.tribblers[deleteListRequest.Key]; !ok {
		re := storagerpc.PutReply{Status: storagerpc.KeyNotFound}
		ss.lock.Unlock()
		LOGF.Printf("delete list finish 2")
		return &re
	}
	flag := false

	for e := ss.tribblers[deleteListRequest.Key].Front(); e != nil; e = e.Next() {
		if e.Value == deleteListRequest.Value {
			flag = true // find whether the item exists
			break
		}
	}
	if !flag {
		re := storagerpc.PutReply{Status: storagerpc.ItemNotFound}
		ss.lock.Unlock()
		LOGF.Printf("delete list finish 3")
		return &re
	}

	ss.cacheLocks[deleteListRequest.Key].Lock()
	ss.lock.Unlock()
	sendRevokeLease(ss, deleteListRequest.Key)

	for e := ss.tribblers[deleteListRequest.Key].Front(); e != nil; e = e.Next() {
		if e.Value == deleteListRequest.Value {
			ss.tribblers[deleteListRequest.Key].Remove(e)
		}
	}

	re := storagerpc.PutReply{Status: storagerpc.OK}
	ss.cacheLocks[deleteListRequest.Key].Unlock()
	LOGF.Printf("delete list finish 4")
	return &re
}

func sendRevokeLease(ss *storageServer, key string) {
	LOGF.Printf("number of revoke : %d", len(ss.keyClientLeaseMap[key]))
	for i := 0; i < len(ss.keyClientLeaseMap[key]); i++ {
		leaseTmp := ss.keyClientLeaseMap[key][i]

		if !time.Now().After(leaseTmp.leaseTime.Add((storagerpc.LeaseGuardSeconds + storagerpc.LeaseSeconds) * time.Second)) {
			cli, err := rpc.DialHTTP("tcp", ss.keyClientLeaseMap[key][i].hostport)
			if err != nil {
				LOGF.Println(err)
				LOGF.Printf("fail to Dail HTTP : %s", ss.keyClientLeaseMap[key][i].hostport)
			}

			revokeLeaseArg := &storagerpc.RevokeLeaseArgs{Key: key}
			revokeLeaseReply := storagerpc.RevokeLeaseReply{}
			cli.Call("LeaseCallbacks.RevokeLease", revokeLeaseArg, &revokeLeaseReply)
			LOGF.Printf("REVOKE : %s", key)
			if revokeLeaseReply.Status != storagerpc.OK {
				LOGF.Printf("revokeLeaseReply status is not OK, key is %s", key)
			}
		}
	}
	ss.keyClientLeaseMap[key] = make([]hasLeaseClient, 0) // clear the lease
}