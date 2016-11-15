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
	nodeID                    uint32
	lowerBound                uint32
	upperBound                uint32

	numNodes                  int
	nodes                     []storagerpc.Node
	nodeIndex                 int
	masterServerHostPort      string
	port                      int

	clients                   map[string]string
	tribblers                 map[string]*list.List

	lock                      *sync.Mutex
	serverLock                *sync.Mutex
	operationLocks            map[string]*sync.Mutex
	operationLocksForList     map[string]*sync.Mutex
	keyClientLeaseMap         map[string][]hasLeaseClient
	keyClientLeaseMapForList  map[string][]hasLeaseClient

	numberOfPut               map[string]int
	numberOfPutForList        map[string]int

	changeOrder               map[string]*list.List
	changeListOrder           map[string]*list.List
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
	newStorageServer.operationLocksForList = make(map[string]*sync.Mutex)
	newStorageServer.keyClientLeaseMap = make(map[string][]hasLeaseClient)
	newStorageServer.keyClientLeaseMapForList = make(map[string][]hasLeaseClient)

	newStorageServer.numberOfPut = make(map[string]int)
	newStorageServer.numberOfPutForList = make(map[string]int)

	newStorageServer.clients = make(map[string]string)
	newStorageServer.tribblers = make(map[string]*list.List)

	newStorageServer.changeOrder = make(map[string]*list.List)
	newStorageServer.changeListOrder = make(map[string]*list.List)

	newStorageServer.nodeID = nodeID
	newStorageServer.upperBound = nodeID
	newStorageServer.lowerBound = 0

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

func (ss *storageServer) GetList(args *storagerpc.GetArgs, reply *storagerpc.GetListReply) error {
	replyTmp := getListRequestFunc(ss, args)
	reply.Status = replyTmp.Status
	reply.Value = replyTmp.Value
	reply.Lease = replyTmp.Lease
	return nil
}

func (ss *storageServer) Put(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	curTime := time.Now()
	ss.lock.Lock()
	initchangeOrderforKey(ss, args.Key)
	addToChangeOrder(ss, args.Key, curTime)
	replyTmp := putRequestFunc(ss, args, curTime)
	reply.Status = replyTmp.Status
	return nil
}

func (ss *storageServer) AppendToList(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	curTime := time.Now()
	ss.lock.Lock()
	initchangeListOrderforKey(ss, args.Key)
	addToChangeListOrder(ss, args.Key, curTime)
	replyTmp := putListRequestFunc(ss, args, curTime)
	reply.Status = replyTmp.Status
	return nil
}

func (ss *storageServer) Delete(args *storagerpc.DeleteArgs, reply *storagerpc.DeleteReply) error {
	curTime := time.Now()
	ss.lock.Lock()
	initchangeOrderforKey(ss, args.Key)
	addToChangeOrder(ss, args.Key, curTime)
	replyTmp := deleteRequestFunc(ss, args, curTime)
	reply.Status = replyTmp.Status
	return nil
}

func (ss *storageServer) RemoveFromList(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	curTime := time.Now()
	ss.lock.Lock()
	initchangeListOrderforKey(ss, args.Key)
	addToChangeListOrder(ss, args.Key, curTime)
	replyTmp := deleteListRequestFunc(ss, args, curTime)
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
	LOGF.Printf("NewStorageServer, numNodes : %d", ss.numNodes)

	initRegister(masterServerHostPort, newStorageServer, numNodes, port, nodeID)

	for {
		select {

		}
	}
}

func addServerFunc(ss *storageServer, addServerRequest *storagerpc.RegisterArgs) *storagerpc.RegisterReply {

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
	return &re
}

func getServerFunc(ss *storageServer, getServerRequest *storagerpc.GetServersArgs) *storagerpc.GetServersReply {

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

func putRequestFunc(ss *storageServer, putRequest *storagerpc.PutArgs, operationTime time.Time) *storagerpc.PutReply {
	LOGF.Printf("put, key is {%s}", putRequest.Key)
	ss.lock.Lock()

	if isWrongServer(ss, putRequest.Key) {
		LOGF.Printf("put finish, WrongServer, key: {%s}, lowerBound: %d, key: %d, upperBound: %d", putRequest.Key, ss.lowerBound, libstore.StoreHash(putRequest.Key), ss.upperBound)
		re := storagerpc.PutReply{Status: storagerpc.WrongServer}
		ss.lock.Unlock()
		return &re
	}

	if _, ok := ss.operationLocks[putRequest.Key]; !ok {
		// it's a new key, init the mutex for this key
		ss.operationLocks[putRequest.Key] = &sync.Mutex{}
		ss.keyClientLeaseMap[putRequest.Key] = make([]hasLeaseClient, 0)
		ss.numberOfPut[putRequest.Key] = 0
	}
	mutexTmp := ss.operationLocks[putRequest.Key]
	ss.lock.Unlock()

	// look for all clients that have been granted the lease
	// lock to grant new leases for the key
	// send revokeLease rpc to all these clients
	// after all rpc received the OK, unlock to grant new leases

	addPutNumberForKey(ss, putRequest.Key)

	sendRevokeLease(ss, putRequest.Key)

	operationLockUntilIsFirst(ss, putRequest.Key, operationTime)

	deleteFirstInChangeOrder(ss, putRequest.Key)

	ss.clients[putRequest.Key] = putRequest.Value
	LOGF.Printf("put finish, key: {%s}, new value: {%s}", putRequest.Key, putRequest.Value)
	mutexTmp.Unlock()

	subPutNumberForKey(ss, putRequest.Key)
	re := storagerpc.PutReply{Status: storagerpc.OK}
	return &re

}

func putListRequestFunc(ss *storageServer, request *storagerpc.PutArgs, operationTime time.Time) *storagerpc.PutReply {
	LOGF.Println("append to list key is {%s}", request.Value)
	ss.lock.Lock()

	if isWrongServer(ss, request.Key) {
		LOGF.Printf("append to list finish, WrongServer, key: {%s}, lowerBound: %d, key: %d, upperBound: %d", request.Key, ss.lowerBound, libstore.StoreHash(request.Key), ss.upperBound)
		re := storagerpc.PutReply{Status: storagerpc.WrongServer}
		ss.lock.Unlock()
		return &re
	}
	if _, ok := ss.operationLocksForList[request.Key]; !ok {
		// it's a new key, init the mutex for this key
		ss.operationLocksForList[request.Key] = &sync.Mutex{}
		ss.keyClientLeaseMapForList[request.Key] = make([]hasLeaseClient, 0)
		ss.numberOfPutForList[request.Key] = 0
	}
	mutexTmp := ss.operationLocksForList[request.Key]
	ss.lock.Unlock()

	operationLockUntilIsFirstForList(ss, request.Key, operationTime)

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
		mutexTmp.Unlock()
		deleteFirstInChangeListOrder(ss, request.Key)
		LOGF.Println("append to list finish, ItemExsits, key: {%s}, value: {%s}", request.Key, request.Value)
		return &re
	}
	mutexTmp.Unlock()

	addPutNumberForKeyForList(ss, request.Key)

	sendRevokeLeaseForList(ss, request.Key)

	operationLockUntilIsFirstForList(ss, request.Key, operationTime)

	deleteFirstInChangeListOrder(ss, request.Key)

	ss.tribblers[request.Key].PushFront(request.Value)
	LOGF.Println("append to list finish, key is {%s}, value is {%s}", request.Key, request.Value)
	mutexTmp.Unlock()

	subPutNumberForKeyForList(ss, request.Key)
	re := storagerpc.PutReply{Status: storagerpc.OK}

	return &re

}

func getRequestFunc(ss *storageServer, request *storagerpc.GetArgs) *storagerpc.GetReply {
	LOGF.Printf("get, key is {%s}", request.Key)
	ss.lock.Lock()

	if isWrongServer(ss, request.Key) {
		LOGF.Printf("get finish, WrongServer, key: {%s}, lowerBound: %d, key: %d, upperBound: %d", request.Key, ss.lowerBound, libstore.StoreHash(request.Key), ss.upperBound)
		re := storagerpc.GetReply{Status: storagerpc.WrongServer}
		ss.lock.Unlock()
		return &re
	}

	if _, ok := ss.operationLocks[request.Key]; !ok {
		re := storagerpc.GetReply{Status: storagerpc.KeyNotFound}
		ss.lock.Unlock()
		LOGF.Printf("get finish, KeyNotFound, key: {%s}", request.Key)
		return &re
	}
	operationLocksTmp := ss.operationLocks[request.Key]
	ss.lock.Unlock()
	operationLocksTmp.Lock() // wait for lease can be granted

	re := storagerpc.GetReply{Status: storagerpc.OK}
	re.Value = ss.clients[request.Key]

	if request.WantLease {
		canGrantLeaseflag := putNumberForKeyIsZero(ss, request.Key)

		if canGrantLeaseflag {
			ss.lock.Lock()
			tmp := append(ss.keyClientLeaseMap[request.Key], hasLeaseClient{leaseTime: time.Now(), hostport: request.HostPort})
			ss.keyClientLeaseMap[request.Key] = tmp
			ss.lock.Unlock()
			LOGF.Printf("get want lease, granted, key: {%s}", request.Key)
			re.Lease.Granted = true
			re.Lease.ValidSeconds = storagerpc.LeaseSeconds
		} else {
			LOGF.Printf("get want lease, not granted, key: {%s}", request.Key)
			re.Lease.Granted = false
		}
	}
	operationLocksTmp.Unlock()
	LOGF.Printf("get finish, key: {%s}, value: {%s}", request.Key, re.Value)
	return &re
}

func getListRequestFunc(ss *storageServer, request *storagerpc.GetArgs) *storagerpc.GetListReply {

	LOGF.Printf("getlist, key: {%s}", request.Key)
	ss.lock.Lock()

	if isWrongServer(ss, request.Key) {
		LOGF.Printf("getlist finish, WrongServer, key: {%s}, lowerBound: %d, key: %d, upperBound: %d", request.Key, ss.lowerBound, libstore.StoreHash(request.Key), ss.upperBound)
		re := storagerpc.GetListReply{Status: storagerpc.WrongServer}
		ss.lock.Unlock()
		return &re
	}

	if _, ok := ss.operationLocksForList[request.Key]; !ok {
		re := storagerpc.GetListReply{Status: storagerpc.KeyNotFound}
		ss.lock.Unlock()
		LOGF.Printf("getlist finish, KeyNotFound, key: {%s}", request.Key)
		return &re
	}
	operationLocksTmp := ss.operationLocksForList[request.Key]
	ss.lock.Unlock()
	operationLocksTmp.Lock() // wait for lease can be granted

	re := storagerpc.GetListReply{Status: storagerpc.OK}
	var str []string
	for e := ss.tribblers[request.Key].Front(); e != nil; e = e.Next() {
		str = append(str, e.Value.(string))
	}
	re.Value = str
	if request.WantLease {

		canGrantLeaseflag := putNumberForKeyIsZeroForList(ss, request.Key)

		if canGrantLeaseflag {
			ss.lock.Lock()
			tmp := append(ss.keyClientLeaseMapForList[request.Key], hasLeaseClient{leaseTime: time.Now(), hostport: request.HostPort})
			ss.keyClientLeaseMapForList[request.Key] = tmp
			ss.lock.Unlock()
			re.Lease.Granted = true
			re.Lease.ValidSeconds = storagerpc.LeaseSeconds
			LOGF.Printf("getlist want lease, granted, key: {%s}", request.Key)

		} else {
			re.Lease.Granted = false
			LOGF.Printf("getlist want lease, not granted, key: {%s}", request.Key)
		}
	}

	operationLocksTmp.Unlock()
	LOGF.Printf("getlist finish, key: {%s}, value: {%s}", request.Key, re.Value)
	return &re
}

func deleteRequestFunc(ss *storageServer, deleteRequest *storagerpc.DeleteArgs, operationTime time.Time) *storagerpc.DeleteReply {
	LOGF.Printf("delete, key: {%s}", deleteRequest.Key)
	ss.lock.Lock()

	if isWrongServer(ss, deleteRequest.Key) {
		LOGF.Printf("delete finish, WrongServer, key: {%s}, lowerBound: %d, key: %d, upperBound: %d", deleteRequest.Key, ss.lowerBound, libstore.StoreHash(deleteRequest.Key), ss.upperBound)
		re := storagerpc.DeleteReply{Status: storagerpc.WrongServer}
		ss.lock.Unlock()
		return &re
	}
	if _, ok := ss.operationLocks[deleteRequest.Key]; !ok {
		re := storagerpc.DeleteReply{Status: storagerpc.KeyNotFound}
		ss.lock.Unlock()
		LOGF.Printf("delete finish, KeyNotFound, key: {%s}", deleteRequest.Key)
		return &re
	}
	operationLocksTmp := ss.operationLocks[deleteRequest.Key]
	ss.lock.Unlock()

	addPutNumberForKey(ss, deleteRequest.Key)

	sendRevokeLease(ss, deleteRequest.Key)

	operationLockUntilIsFirst(ss, deleteRequest.Key, operationTime)

	deleteFirstInChangeOrder(ss, deleteRequest.Key)

	delete(ss.clients, deleteRequest.Key)

	operationLocksTmp.Unlock()

	ss.lock.Lock()
	delete(ss.operationLocks, deleteRequest.Key)
	ss.lock.Unlock()

	//delete(ss.tribblers, deleteRequest.Key)
	re := storagerpc.DeleteReply{Status: storagerpc.OK}

	subPutNumberForKey(ss, deleteRequest.Key)
	LOGF.Printf("delete finish success, key: {%s}", deleteRequest.Key)

	return &re
}

func deleteListRequestFunc(ss *storageServer, deleteListRequest *storagerpc.PutArgs, operationTime time.Time) *storagerpc.PutReply {
	LOGF.Printf("deleteList, key: {%s}", deleteListRequest.Key)
	ss.lock.Lock()

	if isWrongServer(ss, deleteListRequest.Key) {
		LOGF.Printf("deleteList finish, WrongServer, key: {%s}, lowerBound: %d, key: %d, upperBound: %d", deleteListRequest.Key, ss.lowerBound, libstore.StoreHash(deleteListRequest.Key), ss.upperBound)
		re := storagerpc.PutReply{Status: storagerpc.WrongServer}
		ss.lock.Unlock()
		return &re
	}
	if _, ok := ss.operationLocksForList[deleteListRequest.Key]; !ok {
		re := storagerpc.PutReply{Status: storagerpc.KeyNotFound}
		ss.lock.Unlock()
		LOGF.Printf("deleteList finish, KeyNotFound, key: {%s}", deleteListRequest.Key)
		return &re
	}
	operationLocksTmp := ss.operationLocksForList[deleteListRequest.Key]
	ss.lock.Unlock()

	operationLockUntilIsFirstForList(ss, deleteListRequest.Key, operationTime)

	flag := false
	for e := ss.tribblers[deleteListRequest.Key].Front(); e != nil; e = e.Next() {
		if e.Value == deleteListRequest.Value {
			flag = true // find whether the item exists
			break
		}
	}
	if !flag {
		re := storagerpc.PutReply{Status: storagerpc.ItemNotFound}
		operationLocksTmp.Unlock()
		deleteFirstInChangeListOrder(ss, deleteListRequest.Key)
		LOGF.Printf("deleteList finish, ItemNotFound, key: {%s}", deleteListRequest.Key)
		return &re
	}

	operationLocksTmp.Unlock()

	addPutNumberForKeyForList(ss, deleteListRequest.Key)

	sendRevokeLeaseForList(ss, deleteListRequest.Key)

	operationLockUntilIsFirstForList(ss, deleteListRequest.Key, operationTime)

	deleteFirstInChangeListOrder(ss, deleteListRequest.Key)

	for e := ss.tribblers[deleteListRequest.Key].Front(); e != nil; e = e.Next() {
		if e.Value == deleteListRequest.Value {
			ss.tribblers[deleteListRequest.Key].Remove(e)
		}
	}
	operationLocksTmp.Unlock()

	re := storagerpc.PutReply{Status: storagerpc.OK}
	subPutNumberForKeyForList(ss, deleteListRequest.Key)
	LOGF.Printf("deleteList finish successfully, key: {%s}, value: {%s}", deleteListRequest.Key, deleteListRequest.Value)
	return &re
}

func sendRevokeLease(ss *storageServer, key string) {
	ss.lock.Lock()
	keyClientLeaseMapTmp := ss.keyClientLeaseMap[key]
	ss.lock.Unlock()

	var chanTmp chan bool
	revokeSize := len(keyClientLeaseMapTmp)
	LOGF.Printf("Revoke Begin, number of revoke: %d, key: {%s}", revokeSize, key)

	for i := 0; i < revokeSize; i++ {
		leaseTmp := keyClientLeaseMapTmp[i]
		chanTmp = make(chan bool, 100)
		timeNow := time.Now()
		go receiveAMessage(key, leaseTmp, chanTmp, timeNow)

	}
	for i := 0; i < revokeSize; i++ {
		<-chanTmp
	}
	LOGF.Printf("revoke finish, key: {%s}", key)
	ss.lock.Lock()
	ss.keyClientLeaseMap[key] = make([]hasLeaseClient, 0) // clear the lease
	ss.lock.Unlock()
}

func sendRevokeLeaseForList(ss *storageServer, key string) {
	ss.lock.Lock()
	keyClientLeaseMapTmp := ss.keyClientLeaseMapForList[key]
	ss.lock.Unlock()

	var chanTmp chan bool
	revokeSize := len(keyClientLeaseMapTmp)
	LOGF.Printf("Revoke Begin, number of revoke: %d, key: {%s}", revokeSize, key)

	for i := 0; i < revokeSize; i++ {
		leaseTmp := keyClientLeaseMapTmp[i]
		chanTmp = make(chan bool, 100)
		timeNow := time.Now()
		go receiveAMessage(key, leaseTmp, chanTmp, timeNow)

	}
	for i := 0; i < revokeSize; i++ {
		<-chanTmp
	}
	LOGF.Printf("revoke finish, key: {%s}", key)
	ss.lock.Lock()
	ss.keyClientLeaseMapForList[key] = make([]hasLeaseClient, 0) // clear the lease
	ss.lock.Unlock()
}

func receiveAMessage(key string, leaseTmp hasLeaseClient, chanTmp chan bool, timeNow time.Time) {

	if timeNow.After(leaseTmp.leaseTime.Add((storagerpc.LeaseGuardSeconds + storagerpc.LeaseSeconds) * time.Second)) {
		LOGF.Printf("REVOKE time out before send revoke: key: {%s}", key)
		chanTmp <- true
		return
	}

	revokeReplyChan := make(chan bool, 1)
	go sendARevoke(key, leaseTmp, revokeReplyChan)

	duration := -timeNow.Sub(leaseTmp.leaseTime.Add((storagerpc.LeaseGuardSeconds + storagerpc.LeaseSeconds) * time.Second))

	LOGF.Println(duration)
	select {
	case <-time.After(duration):
		LOGF.Printf("Ignore REVOKE's reply, key: {%s}", key)
		chanTmp <- true
	case <-revokeReplyChan:
		LOGF.Printf("REVOKE call reply, key: {%s}", key)
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
		LOGF.Printf("RevokeLeaseReply status is not OK, key: {%s}", key)
	}
	revokeReplyChan <- true
}

func isWrongServer(ss *storageServer, key string) bool {
	hashVal := libstore.StoreHash(key)
	if (ss.upperBound > ss.lowerBound && (hashVal < ss.lowerBound || hashVal > ss.upperBound)) ||
		(hashVal > ss.upperBound && hashVal < ss.lowerBound) {
		return true
	} else {
		return false
	}
}

func addPutNumberForKey(ss *storageServer, key string) {
	ss.lock.Lock()
	ss.numberOfPut[key] ++
	ss.lock.Unlock()
}

func addPutNumberForKeyForList(ss *storageServer, key string) {
	ss.lock.Lock()
	ss.numberOfPutForList[key] ++
	ss.lock.Unlock()

}

func subPutNumberForKey(ss *storageServer, key string) {
	ss.lock.Lock()
	ss.numberOfPut[key] --
	ss.lock.Unlock()
}

func subPutNumberForKeyForList(ss *storageServer, key string) {
	ss.lock.Lock()
	ss.numberOfPutForList[key] --
	ss.lock.Unlock()
}

func putNumberForKeyIsZero(ss *storageServer, key string) bool {
	re := false
	ss.lock.Lock()
	if ss.numberOfPut[key] == 0 {
		re = true
	}
	ss.lock.Unlock()

	return re
}

func putNumberForKeyIsZeroForList(ss *storageServer, key string) bool {
	re := false
	ss.lock.Lock()
	if ss.numberOfPutForList[key] == 0 {
		re = true
	}
	ss.lock.Unlock()

	return re
}

func initchangeOrderforKey(ss *storageServer, key string) {
	if _, ok := ss.changeOrder[key]; !ok {
		ss.changeOrder[key] = list.New()
	}
	ss.lock.Unlock()
}

func initchangeListOrderforKey(ss *storageServer, key string) {
	if _, ok := ss.changeListOrder[key]; !ok {
		ss.changeListOrder[key] = list.New()
	}
	ss.lock.Unlock()
}

func addToChangeOrder(ss *storageServer, key string, curTime time.Time) {
	ss.lock.Lock()
	ss.changeOrder[key].PushBack(curTime)
	ss.lock.Unlock()
}

func addToChangeListOrder(ss *storageServer, key string, curTime time.Time) {
	ss.lock.Lock()
	ss.changeListOrder[key].PushBack(curTime)
	ss.lock.Unlock()
}

func deleteFirstInChangeOrder(ss *storageServer, key string) {
	ss.lock.Lock()
	ss.changeOrder[key].Remove(ss.changeOrder[key].Front())
	ss.lock.Unlock()
}

func deleteFirstInChangeListOrder(ss *storageServer, key string) {
	ss.lock.Lock()
	ss.changeListOrder[key].Remove(ss.changeListOrder[key].Front())
	ss.lock.Unlock()
}

func isFirstInChangeOrder(ss *storageServer, key string, timeTmp time.Time) bool {
	re := false
	ss.lock.Lock()
	if timeTmp.Equal(ss.changeOrder[key].Front().Value.(time.Time)) {
		re = true
	}
	ss.lock.Unlock()
	return re
}

func isFirstInChangeListOrder(ss *storageServer, key string, timeTmp time.Time) bool {
	re := false
	ss.lock.Lock()
	if timeTmp.Equal(ss.changeListOrder[key].Front().Value.(time.Time)) {
		re = true
	}
	ss.lock.Unlock()
	return re
}

func operationLockUntilIsFirst(ss *storageServer, key string, myOperationTime time.Time) {
	ss.lock.Lock()
	mutexTmp := ss.operationLocks[key]
	ss.lock.Unlock()

	for {
		mutexTmp.Lock()
		if isFirstInChangeOrder(ss, key, myOperationTime) {
			break
		} else {
			mutexTmp.Unlock()
		}
	}
}

func operationLockUntilIsFirstForList(ss *storageServer, key string, myOperationTime time.Time) {
	ss.lock.Lock()
	mutexTmp := ss.operationLocksForList[key]
	ss.lock.Unlock()

	for {
		mutexTmp.Lock()
		if isFirstInChangeListOrder(ss, key, myOperationTime) {
			break
		} else {
			mutexTmp.Unlock()
		}
	}
}

