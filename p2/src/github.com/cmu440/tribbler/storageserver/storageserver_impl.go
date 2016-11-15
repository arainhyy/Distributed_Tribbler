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
	lowerBound           uint32
	upperBound           uint32

	numNodes             int
	nodes                []storagerpc.Node
	nodeIndex            int
	masterServerHostPort string
	port                 int

	clients              map[string]string
	tribblers            map[string]*list.List

	lock                 *sync.Mutex
	serverLock           *sync.Mutex
	operationLocks       map[string]*sync.Mutex

	numberOfPut          map[string]int
	numberOfPutLock      map[string]*sync.Mutex
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
	newStorageServer.lock = &sync.Mutex{}
	newStorageServer.lock.Lock()
	newStorageServer.serverLock = &sync.Mutex{}

	newStorageServer.operationLocks = make(map[string]*sync.Mutex)
	newStorageServer.keyClientLeaseMap = make(map[string][]hasLeaseClient)

	newStorageServer.numberOfPut = make(map[string]int)
	newStorageServer.numberOfPutLock = make(map[string]*sync.Mutex)

	newStorageServer.clients = make(map[string]string)
	newStorageServer.tribblers = make(map[string]*list.List)

	newStorageServer.nodeID = nodeID
	newStorageServer.upperBound = nodeID
	newStorageServer.lowerBound = 0
	//newStorageServer.lowerBound = 4294967295
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

	go storageServerRoutine(newStorageServer, masterServerHostPort, newStorageServer, numNodes, port, nodeID)
	println("finish")

	return newStorageServer, nil
}

func initRegister(masterServerHostPort string, newStorageServer *storageServer, numNodes, port int, nodeID uint32) {
	newStorageServer.nodes = make([]storagerpc.Node, numNodes, numNodes)
	if len(masterServerHostPort) == 0 {
		newStorageServer.nodes[0].HostPort = fmt.Sprintf(":%d", port)
		newStorageServer.nodes[0].NodeID = nodeID
		newStorageServer.nodeIndex = 1
		if numNodes == 1 {
			newStorageServer.lock.Unlock()
			newStorageServer.lowerBound = newStorageServer.upperBound + 1
		}

		println("init master")

	} else {
		println("init slave")
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
				newStorageServer.numNodes = len(reply.Servers)
				newStorageServer.nodes = reply.Servers
				max := newStorageServer.upperBound
				for i := 0; i < len(newStorageServer.nodes); i++ {
					//LOGF.Printf("server %d : nodeID: %d, hostport : %s", i, newStorageServer.nodes[i].NodeID, newStorageServer.nodes[i].HostPort)
					if newStorageServer.nodes[i].NodeID < newStorageServer.upperBound &&
						newStorageServer.nodes[i].NodeID > newStorageServer.lowerBound {
						newStorageServer.lowerBound = newStorageServer.nodes[i].NodeID + 1
					}
					if newStorageServer.nodes[i].NodeID > max {
						max = newStorageServer.nodes[i].NodeID
					}
				}
				if newStorageServer.lowerBound == 0 {
					newStorageServer.lowerBound = max + 1
				}
				//LOGF.Printf("server %d : nodeID: %d, hostport : %s, lowerBound: %d, upperBound: %d", newStorageServer.nodeID, newStorageServer.port, newStorageServer.lowerBound, newStorageServer.upperBound)
				break
			} else {
				time.Sleep(1000 * time.Millisecond)
			}
		}
		newStorageServer.lock.Unlock()
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

func storageServerRoutine(ss *storageServer, masterServerHostPort string, newStorageServer *storageServer, numNodes, port int, nodeID uint32) {
	const (
		name = "log.txt"
		//
		flag = os.O_APPEND | os.O_WRONLY
		//flag = os.O_RDWR | os.O_CREATE
		perm = os.FileMode(0666)
	)

	file, _ := os.OpenFile(name, flag, perm)

	defer file.Close()

	LOGF = log.New(file, "", log.Lshortfile | log.Lmicroseconds)
	LOGF.Printf("NewStorageServer")
	LOGF.Printf("numNodes : %d", ss.numNodes)

	initRegister(masterServerHostPort, newStorageServer, numNodes, port, nodeID)

	for {
		select {

		}
	}
}

func addServerFunc(ss *storageServer, addServerRequest *storagerpc.RegisterArgs) *storagerpc.RegisterReply {

	LOGF.Print("add Server")
	ss.serverLock.Lock()
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
		LOGF.Printf("register one, nodeIndex %d, numNodes %d", ss.nodeIndex, ss.numNodes)
		if ss.nodeIndex == ss.numNodes {
			ss.lock.Unlock()
		}
	}

	re := storagerpc.RegisterReply{}
	re.Servers = ss.nodes
	if ss.nodeIndex >= ss.numNodes {
		re.Status = storagerpc.OK
	} else {
		re.Status = storagerpc.NotReady
	}

	max := ss.upperBound
	for i := 0; i < len(ss.nodes); i++ {
		LOGF.Printf("server %d : nodeID: %d, hostport : %s", i, ss.nodes[i].NodeID, ss.nodes[i].HostPort)
		if ss.nodes[i].NodeID < ss.upperBound &&
			ss.nodes[i].NodeID > ss.lowerBound {
			ss.lowerBound = ss.nodes[i].NodeID + 1
		}
		if ss.nodes[i].NodeID > max {
			max = ss.nodes[i].NodeID
		}
	}
	if ss.lowerBound == 0 {
		ss.lowerBound = max + 1
	}
	ss.serverLock.Unlock()
	LOGF.Printf("add Server finish")
	return &re
}

func getServerFunc(ss *storageServer, getServerRequest *storagerpc.GetServersArgs) *storagerpc.GetServersReply {

	LOGF.Printf("get Server")
	ss.serverLock.Lock()
	// getServerRequest is empty
	re := storagerpc.GetServersReply{}
	re.Servers = ss.nodes
	if ss.nodeIndex == ss.numNodes {
		re.Status = storagerpc.OK
		LOGF.Printf("get Server returns OK")

	} else {
		re.Status = storagerpc.NotReady
		LOGF.Printf("get Server returns NotReady")
	}
	ss.serverLock.Unlock()
	return &re
}

func putRequestFunc(ss *storageServer, putRequest *storagerpc.PutArgs) *storagerpc.PutReply {
	LOGF.Printf("put, key is {%s}", putRequest.Key)
	ss.lock.Lock()
	hashVal := libstore.StoreHash(putRequest.Key)
	if (ss.upperBound > ss.lowerBound && (hashVal < ss.lowerBound || hashVal > ss.upperBound)) ||
		(hashVal > ss.upperBound && hashVal < ss.lowerBound) {
		LOGF.Printf("put finish, WrongServer, key: {%s}, lowerBound: %d, key: %d, upperBound: %d", putRequest.Key, ss.lowerBound, hashVal, ss.upperBound)
		re := storagerpc.PutReply{Status: storagerpc.WrongServer}
		ss.lock.Unlock()
		return &re
	}

	if _, ok := ss.operationLocks[putRequest.Key]; !ok {
		// it's a new key, init the mutex for this key
		ss.operationLocks[putRequest.Key] = &sync.Mutex{}
		ss.keyClientLeaseMap[putRequest.Key] = make([]hasLeaseClient, 0)
		ss.numberOfPut[putRequest.Key] = 0
		ss.numberOfPutLock[putRequest.Key] = &sync.Mutex{}
	}

	ss.lock.Unlock()

	// look for all clients that have been granted the lease
	// lock to grant new leases for the key
	// send revokeLease rpc to all these clients
	// after all rpc received the OK, unlock to grant new leases


	ss.numberOfPutLock[putRequest.Key].Lock()
	ss.numberOfPut[putRequest.Key] ++
	ss.numberOfPutLock[putRequest.Key].Unlock()

	LOGF.Printf("begin revoke")
	sendRevokeLease(ss, putRequest.Key)

	ss.operationLocks[putRequest.Key].Lock()
	ss.clients[putRequest.Key] = putRequest.Value
	LOGF.Printf("put finish, key: {%s}, new value: {%s}", putRequest.Key, putRequest.Value)
	ss.operationLocks[putRequest.Key].Unlock()

	ss.numberOfPutLock[putRequest.Key].Lock()
	ss.numberOfPut[putRequest.Key] --
	ss.numberOfPutLock[putRequest.Key].Unlock()
	re := storagerpc.PutReply{Status: storagerpc.OK}
	return &re

}

func putListRequestFunc(ss *storageServer, request *storagerpc.PutArgs) *storagerpc.PutReply {
	LOGF.Println("append to list key is {%s}", request.Value)
	ss.lock.Lock()
	hashVal := libstore.StoreHash(request.Key)
	if (ss.upperBound > ss.lowerBound && (hashVal < ss.lowerBound || hashVal > ss.upperBound)) ||
		(hashVal > ss.upperBound && hashVal < ss.lowerBound) {
		LOGF.Printf("append to list finish, WrongServer, key: {%s}, lowerBound: %d, key: %d, upperBound: %d", request.Key, ss.lowerBound, hashVal, ss.upperBound)
		re := storagerpc.PutReply{Status: storagerpc.WrongServer}
		ss.lock.Unlock()
		return &re
	}
	if _, ok := ss.operationLocks[request.Key]; !ok {
		// it's a new key, init the mutex for this key
		ss.operationLocks[request.Key] = &sync.Mutex{}
		ss.keyClientLeaseMap[request.Key] = make([]hasLeaseClient, 0)
		ss.numberOfPut[request.Key] = 0
		ss.numberOfPutLock[request.Key] = &sync.Mutex{}
	}
	ss.lock.Unlock()

	ss.operationLocks[request.Key].Lock()
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
		ss.operationLocks[request.Key].Unlock()
		LOGF.Println("append to list finish, ItemExsits, key: {%s}, value: {%s}", request.Key, request.Value)
		return &re
	}
	ss.operationLocks[request.Key].Unlock()

	ss.numberOfPutLock[request.Key].Lock()
	ss.numberOfPut[request.Key] ++
	ss.numberOfPutLock[request.Key].Unlock()

	sendRevokeLease(ss, request.Key)

	ss.operationLocks[request.Key].Lock()
	ss.tribblers[request.Key].PushFront(request.Value)
	LOGF.Println("append to list finish, key is {%s}, value is {%s}", request.Key, request.Value)
	ss.operationLocks[request.Key].Unlock()

	ss.numberOfPutLock[request.Key].Lock()
	ss.numberOfPut[request.Key] --
	ss.numberOfPutLock[request.Key].Unlock()
	re := storagerpc.PutReply{Status: storagerpc.OK}

	return &re

}

func getRequestFunc(ss *storageServer, request *storagerpc.GetArgs) *storagerpc.GetReply {
	LOGF.Printf("get, key is {%s}", request.Key)
	ss.lock.Lock()
	hashVal := libstore.StoreHash(request.Key)
	if (ss.upperBound > ss.lowerBound && (hashVal < ss.lowerBound || hashVal > ss.upperBound)) ||
		(hashVal > ss.upperBound && hashVal < ss.lowerBound) {
		LOGF.Printf("get finish, WrongServer, key: {%s}, lowerBound: %d, key: %d, upperBound: %d", request.Key, ss.lowerBound, hashVal, ss.upperBound)
		re := storagerpc.GetReply{Status: storagerpc.WrongServer}
		ss.lock.Unlock()
		return &re
	}

	if _, ok := ss.clients[request.Key]; !ok {
		re := storagerpc.GetReply{Status: storagerpc.KeyNotFound}
		ss.lock.Unlock()
		LOGF.Printf("get finish, KeyNotFound, key: {%s}", request.Key)
		return &re
	}

	ss.lock.Unlock()
	ss.operationLocks[request.Key].Lock() // wait for lease can be granted

	re := storagerpc.GetReply{Status: storagerpc.OK}
	re.Value = ss.clients[request.Key]

	if request.WantLease {
		canGrantLeaseflag := false
		ss.numberOfPutLock[request.Key].Lock()
		if ss.numberOfPut[request.Key] == 0 {
			canGrantLeaseflag = true
		}
		ss.numberOfPutLock[request.Key].Unlock()
		if canGrantLeaseflag {
			tmp := append(ss.keyClientLeaseMap[request.Key], hasLeaseClient{leaseTime: time.Now(), hostport: request.HostPort})
			ss.keyClientLeaseMap[request.Key] = tmp
			LOGF.Printf("get want lease, granted, key: {%s}", request.Key)
			re.Lease.Granted = true
			re.Lease.ValidSeconds = storagerpc.LeaseSeconds
		} else {
			LOGF.Printf("get want lease, not granted, key: {%s}", request.Key)
			re.Lease.Granted = false
		}
	}
	ss.operationLocks[request.Key].Unlock()
	LOGF.Printf("get finish, key: {%s}, value: {%s}", request.Key, re.Value)
	return &re
}

func getListRequestFunc(ss *storageServer, request *storagerpc.GetArgs) *storagerpc.GetListReply {

	LOGF.Printf("getlist, key: {%s}", request.Key)
	ss.lock.Lock()
	hashVal := libstore.StoreHash(request.Key)
	if (ss.upperBound > ss.lowerBound && (hashVal < ss.lowerBound || hashVal > ss.upperBound)) ||
		(hashVal > ss.upperBound && hashVal < ss.lowerBound) {
		LOGF.Printf("getlist finish, WrongServer, key: {%s}, lowerBound: %d, key: %d, upperBound: %d", request.Key, ss.lowerBound, hashVal, ss.upperBound)
		re := storagerpc.GetListReply{Status: storagerpc.WrongServer}
		ss.lock.Unlock()
		return &re
	}

	if _, ok := ss.tribblers[request.Key]; !ok {
		re := storagerpc.GetListReply{Status: storagerpc.KeyNotFound}
		ss.lock.Unlock()
		LOGF.Printf("getlist finish, KeyNotFound, key: {%s}", request.Key)
		return &re
	}
	ss.lock.Unlock()

	ss.operationLocks[request.Key].Lock() // wait for lease can be granted
	re := storagerpc.GetListReply{Status: storagerpc.OK}
	var str []string
	for e := ss.tribblers[request.Key].Front(); e != nil; e = e.Next() {
		str = append(str, e.Value.(string))
	}
	re.Value = str
	if request.WantLease {
		canGrantLeaseflag := false
		ss.numberOfPutLock[request.Key].Lock()
		if ss.numberOfPut[request.Key] == 0 {
			canGrantLeaseflag = true
		}
		ss.numberOfPutLock[request.Key].Unlock()
		if canGrantLeaseflag {
			tmp := append(ss.keyClientLeaseMap[request.Key], hasLeaseClient{leaseTime: time.Now(), hostport: request.HostPort})
			ss.keyClientLeaseMap[request.Key] = tmp
			re.Lease.Granted = true
			re.Lease.ValidSeconds = storagerpc.LeaseSeconds
			LOGF.Printf("getlist want lease, granted, key: {%s}", request.Key)
		} else {
			re.Lease.Granted = false
			LOGF.Printf("getlist want lease, not granted, key: {%s}", request.Key)
		}
	}

	ss.operationLocks[request.Key].Unlock()
	LOGF.Printf("getlist finish, key: {%s}, value: {%s}", request.Key, re.Value)
	return &re
}

func deleteRequestFunc(ss *storageServer, deleteRequest *storagerpc.DeleteArgs) *storagerpc.DeleteReply {
	LOGF.Printf("delete, key: {%s}", deleteRequest.Key)
	ss.lock.Lock()

	hashVal := libstore.StoreHash(deleteRequest.Key)
	if (ss.upperBound > ss.lowerBound && (hashVal < ss.lowerBound || hashVal > ss.upperBound)) ||
		(hashVal > ss.upperBound && hashVal < ss.lowerBound) {
		LOGF.Printf("delete finish, WrongServer, key: {%s}, lowerBound: %d, key: %d, upperBound: %d", deleteRequest.Key, ss.lowerBound, hashVal, ss.upperBound)
		re := storagerpc.DeleteReply{Status: storagerpc.WrongServer}
		ss.lock.Unlock()
		return &re
	}

	if _, ok := ss.clients[deleteRequest.Key]; !ok {
		re := storagerpc.DeleteReply{Status: storagerpc.KeyNotFound}
		ss.lock.Unlock()
		LOGF.Printf("delete finish, KeyNotFound, key: {%s}", deleteRequest.Key)
		return &re
	}

	ss.lock.Unlock()

	ss.numberOfPutLock[deleteRequest.Key].Lock()
	ss.numberOfPut[deleteRequest.Key] ++
	ss.numberOfPutLock[deleteRequest.Key].Unlock()
	sendRevokeLease(ss, deleteRequest.Key)

	ss.operationLocks[deleteRequest.Key].Lock()
	delete(ss.clients, deleteRequest.Key)
	ss.operationLocks[deleteRequest.Key].Unlock()

	//delete(ss.tribblers, deleteRequest.Key)
	re := storagerpc.DeleteReply{Status: storagerpc.OK}

	ss.numberOfPutLock[deleteRequest.Key].Lock()
	ss.numberOfPut[deleteRequest.Key] --
	ss.numberOfPutLock[deleteRequest.Key].Unlock()
	LOGF.Printf("delete finish success, key: {%s}", deleteRequest.Key)

	return &re
}

func deleteListRequestFunc(ss *storageServer, deleteListRequest *storagerpc.PutArgs) *storagerpc.PutReply {
	LOGF.Printf("deleteList, key: {%s}", deleteListRequest.Key)
	ss.lock.Lock()

	hashVal := libstore.StoreHash(deleteListRequest.Key)
	if (ss.upperBound > ss.lowerBound && (hashVal < ss.lowerBound || hashVal > ss.upperBound)) ||
		(hashVal > ss.upperBound && hashVal < ss.lowerBound) {
		LOGF.Printf("deleteList finish, WrongServer, key: {%s}, lowerBound: %d, key: %d, upperBound: %d", deleteListRequest.Key, ss.lowerBound, hashVal, ss.upperBound)
		re := storagerpc.PutReply{Status: storagerpc.WrongServer}
		ss.lock.Unlock()
		return &re
	}
	ss.lock.Unlock()

	ss.operationLocks[deleteListRequest.Key].Lock()

	if _, ok := ss.tribblers[deleteListRequest.Key]; !ok {
		re := storagerpc.PutReply{Status: storagerpc.KeyNotFound}
		ss.operationLocks[deleteListRequest.Key].Unlock()
		LOGF.Printf("deleteList finish, KeyNotFound, key: {%s}", deleteListRequest.Key)
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
		ss.operationLocks[deleteListRequest.Key].Unlock()
		LOGF.Printf("deleteList finish, ItemNotFound, key: {%s}", deleteListRequest.Key)
		return &re
	}

	ss.operationLocks[deleteListRequest.Key].Unlock()


	ss.numberOfPutLock[deleteListRequest.Key].Lock()
	ss.numberOfPut[deleteListRequest.Key] ++
	ss.numberOfPutLock[deleteListRequest.Key].Unlock()

	sendRevokeLease(ss, deleteListRequest.Key)

	ss.operationLocks[deleteListRequest.Key].Lock()
	for e := ss.tribblers[deleteListRequest.Key].Front(); e != nil; e = e.Next() {
		if e.Value == deleteListRequest.Value {
			ss.tribblers[deleteListRequest.Key].Remove(e)
		}
	}
	ss.operationLocks[deleteListRequest.Key].Unlock()

	re := storagerpc.PutReply{Status: storagerpc.OK}
	ss.numberOfPutLock[deleteListRequest.Key].Lock()
	ss.numberOfPut[deleteListRequest.Key] --
	ss.numberOfPutLock[deleteListRequest.Key].Unlock()
	LOGF.Printf("deleteList finish successfully, key: {%s}, value: {%s}", deleteListRequest.Key, deleteListRequest.Value)
	return &re
}

func sendRevokeLease(ss *storageServer, key string) {
	LOGF.Printf("number of revoke : %d", len(ss.keyClientLeaseMap[key]))
	var chanTmp chan bool
	revokeSize := len(ss.keyClientLeaseMap[key])
	for i := 0; i < revokeSize; i++ {
		leaseTmp := ss.keyClientLeaseMap[key][i]
		chanTmp = make(chan bool, 100)
		timeNow := time.Now()
		go receiveAMessage(key, leaseTmp, chanTmp, timeNow)

	}
	for i := 0; i < revokeSize; i++ {
		<-chanTmp
	}
	LOGF.Printf("revoke finish, key: %s", key)
	ss.keyClientLeaseMap[key] = make([]hasLeaseClient, 0) // clear the lease
}

func receiveAMessage(key string, leaseTmp hasLeaseClient, chanTmp chan bool, timeNow time.Time) {

	if timeNow.After(leaseTmp.leaseTime.Add((storagerpc.LeaseGuardSeconds + storagerpc.LeaseSeconds) * time.Second)) {
		LOGF.Printf("REVOKE time out before send revoke: %s", key)
		chanTmp <- true
		return
	}

	revokeReplyChan := make(chan bool, 1)
	go sendARevoke(key, leaseTmp, revokeReplyChan)

	duration := -timeNow.Sub(leaseTmp.leaseTime.Add((storagerpc.LeaseGuardSeconds + storagerpc.LeaseSeconds) * time.Second))

	LOGF.Println(duration)
	select {
	case <-time.After(duration):
		LOGF.Printf("REVOKE time out: %s", key)
		chanTmp <- true
	case <-revokeReplyChan:
		LOGF.Printf("REVOKE call reply: %s", key)
		chanTmp <- true
	}
}

func sendARevoke(key string, leaseTmp hasLeaseClient, revokeReplyChan chan bool) {
	cli, err := rpc.DialHTTP("tcp", leaseTmp.hostport)
	if err != nil {
		LOGF.Println(err)
		LOGF.Printf("fail to Dail HTTP : %s", leaseTmp.hostport)
	}

	revokeLeaseArg := &storagerpc.RevokeLeaseArgs{Key: key}
	revokeLeaseReply := storagerpc.RevokeLeaseReply{}
	cli.Call("LeaseCallbacks.RevokeLease", revokeLeaseArg, &revokeLeaseReply)
	if revokeLeaseReply.Status != storagerpc.OK {
		LOGF.Printf("revokeLeaseReply status is not OK, key is %s", key)
	}
	revokeReplyChan <- true
}
