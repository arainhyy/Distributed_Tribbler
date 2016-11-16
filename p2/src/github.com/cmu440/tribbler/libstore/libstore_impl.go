package libstore

import (
	"errors"
	"fmt"
	"github.com/cmu440/tribbler/rpc/librpc"
	"github.com/cmu440/tribbler/rpc/storagerpc"
	"net/rpc"
	"sort"
	"sync"
	"time"
)

type libstore struct {
	myHostPort           string
	masterServerHostPort string
	mode                 LeaseMode
	client               *rpc.Client
	storageServerNodes   []storagerpc.Node
	connections          map[string]*rpc.Client
	queryTimestamps      map[string][]time.Time
	leases               map[string]storagerpc.Lease
	cache                map[string]interface{}
	lock                 *sync.Mutex
}

// ByID implements sort.Interface for []storagerpc.Node based on
// the ID field.
type ByID []storagerpc.Node

func (a ByID) Len() int {
	return len(a)
}
func (a ByID) Swap(i, j int) {
	a[i], a[j] = a[j], a[i]
}
func (a ByID) Less(i, j int) bool {
	return a[j].NodeID > a[i].NodeID
}

// NewLibstore creates a new instance of a TribServer's libstore. masterServerHostPort
// is the master storage server's host:port. myHostPort is this Libstore's host:port
// (i.e. the callback address that the storage servers should use to send back
// notifications when leases are revoked).
//
// The mode argument is a debugging flag that determines how the Libstore should
// request/handle leases. If mode is Never, then the Libstore should never request
// leases from the storage server (i.e. the GetArgs.WantLease field should always
// be set to false). If mode is Always, then the Libstore should always request
// leases from the storage server (i.e. the GetArgs.WantLease field should always
// be set to true). If mode is Normal, then the Libstore should make its own
// decisions on whether or not a lease should be requested from the storage server,
// based on the requirements specified in the project PDF handout.  Note that the
// value of the mode flag may also determine whether or not the Libstore should
// register to receive RPCs from the storage servers.
//
// To register the Libstore to receive RPCs from the storage servers, the following
// line of code should suffice:
//
//     rpc.RegisterName("LeaseCallbacks", librpc.Wrap(libstore))
//
// Note that unlike in the NewTribServer and NewStorageServer functions, there is no
// need to create a brand new HTTP handler to serve the requests (the Libstore may
// simply reuse the TribServer's HTTP handler since the two run in the same process).
func NewLibstore(masterServerHostPort, myHostPort string, mode LeaseMode) (Libstore, error) {
	cli, err := rpc.DialHTTP("tcp", masterServerHostPort)
	if err != nil {
		return nil, err
	}
	libstore := &libstore{client: cli}
	libstore.myHostPort = myHostPort
	libstore.masterServerHostPort = masterServerHostPort
	libstore.mode = mode
	libstore.connections = make(map[string]*rpc.Client)
	libstore.queryTimestamps = make(map[string][]time.Time)
	libstore.leases = make(map[string]storagerpc.Lease)
	libstore.cache = make(map[string]interface{})
	libstore.lock = &sync.Mutex{}
	libstore.connections[masterServerHostPort] = cli
	// Get all storage server nodes.
	tryTimes := 0
	args := &storagerpc.GetServersArgs{}
	reply := &storagerpc.GetServersReply{}
	for tryTimes < 5 {
		cli.Call("StorageServer.GetServers", args, reply)
		if reply.Status == storagerpc.OK {
			fmt.Println("reply OK")
			break
		}
		fmt.Println(reply.Status)
		tryTimes++
		time.Sleep(1000 * time.Millisecond)
	}
	libstore.storageServerNodes = reply.Servers
	sort.Sort(ByID(libstore.storageServerNodes))
	// upbound := len(libstore.storageServerNodes)
	// i := 1
	// fmt.Println("before try")
	// for i < upbound {
	// 	node := libstore.storageServerNodes[i]
	// 	conn, _ := rpc.DialHTTP("tcp", node.HostPort)
	// 	libstore.connections[node.HostPort] = conn
	// }
	//lib := new(librpc.RemoteLeaseCallbacks)
	rpc.RegisterName("LeaseCallbacks", librpc.Wrap(libstore))
	return libstore, nil
}

func (ls *libstore) CheckQueryTimestamp(key string) bool {
	ls.lock.Lock()
	_, ok := ls.queryTimestamps[key]
	// If no query of this key before, add entry for this key.
	if !ok {
		queryTimes := make([]time.Time, 0)
		ls.queryTimestamps[key] = queryTimes
	}
	// Add this time to timestamp list.
	timestamps, _ := ls.queryTimestamps[key]
	list := append(timestamps, time.Now())
	ls.queryTimestamps[key] = list
	ls.lock.Unlock()
	// fmt.Println("len list: ", len(list))
	if len(list) < storagerpc.QueryCacheThresh {
		return false
	}
	firstTime := list[len(list)-storagerpc.QueryCacheThresh]
	newList := list[len(list)-storagerpc.QueryCacheThresh:]
	ls.lock.Lock()
	ls.queryTimestamps[key] = newList
	ls.lock.Unlock()
	if time.Since(firstTime).Seconds() < storagerpc.QueryCacheSeconds {
		return true
	}
	return false
}

func (ls *libstore) DeleteCacheLineWhenTimeout(key string) {
	ls.lock.Lock()
	lease := ls.leases[key]
	ls.lock.Unlock()
	<-time.After(time.Duration(lease.ValidSeconds) * time.Second)
	ls.lock.Lock()
	delete(ls.cache, key)
	delete(ls.leases, key)
	ls.lock.Unlock()
}

func (ls *libstore) Get(key string) (string, error) {
	// fmt.Println("!!Get", key)
	// Check cache first.
	ls.lock.Lock()
	value, ok := ls.cache[key]
	if ok {
		lease, _ := ls.leases[key]
		if lease.Granted {
			ls.lock.Unlock()
			return value.(string), nil
		}
	}
	ls.lock.Unlock()
	wantLease := ls.CheckQueryTimestamp(key)
	if ls.mode == Always {
		wantLease = true
	} else if ls.mode == Never {
		wantLease = false
	}
	// fmt.Println("wantlease:", wantLease)
	args := &storagerpc.GetArgs{Key: key, WantLease: wantLease, HostPort: ls.myHostPort}
	var reply storagerpc.GetReply
	conn := ls.GetStorageServerConn(key)
	if err := conn.Call("StorageServer.Get", args, &reply); err != nil {
		// fmt.Println("Get finish, call error", key)
		return "", err
	} else if reply.Status == storagerpc.KeyNotFound {
		// fmt.Println("Get finish, KeyNotFound", key)
		return "", errors.New("GET operation failed with KeyNotFound")
	} else {
		if wantLease && reply.Lease.Granted {
			ls.lock.Lock()
			ls.cache[key] = reply.Value
			ls.leases[key] = reply.Lease
			ls.lock.Unlock()
			go ls.DeleteCacheLineWhenTimeout(key)
		}
		// fmt.Println("Get finish", key)
		return reply.Value, nil
	}

}

func (ls *libstore) Put(key, value string) error {
	// fmt.Println("put", key, value)
	args := &storagerpc.PutArgs{Key: key, Value: value}
	var reply storagerpc.PutReply
	err := ls.GetStorageServerConn(key).Call("StorageServer.Put", args, &reply)
	if err != nil {
		// fmt.Println("put finish call err", key)
		return err
	} else if reply.Status == storagerpc.KeyNotFound {
		// fmt.Println("put finish KeyNotFound", key)
		return errors.New("PUT operation failed with KeyNotFound")
	} else if reply.Status == storagerpc.WrongServer {
		// fmt.Println("put finish WrongServer", key)
		return errors.New("PUT operation failed with WrongServer")
	} else {
		// fmt.Println("put finish", key)
		return nil
	}

}

func (ls *libstore) Delete(key string) error {
	args := &storagerpc.DeleteArgs{Key: key}
	var reply storagerpc.DeleteReply
	if err := ls.GetStorageServerConn(key).Call("StorageServer.Delete", args, &reply); err != nil {
		return err
	} else if reply.Status == storagerpc.KeyNotFound {
		return errors.New("Delete operation failed with KeyNotFound")
	} else if reply.Status == storagerpc.WrongServer {
		return errors.New("Delete operation failed with WrongServer")
	} else {
		return nil
	}
}

func (ls *libstore) GetList(key string) ([]string, error) {
	// Check cache first.
	//fmt.Println("GetList", key)
	ls.lock.Lock()
	value, ok := ls.cache[key]
	if ok {
		lease, _ := ls.leases[key]
		if lease.Granted {
			ls.lock.Unlock()
			return value.([]string), nil
		}
	}
	ls.lock.Unlock()
	wantLease := ls.CheckQueryTimestamp(key)
	if ls.mode == Always {
		wantLease = true
	} else if ls.mode == Never {
		wantLease = false
	}

	args := &storagerpc.GetArgs{Key: key, WantLease: wantLease, HostPort: ls.myHostPort}
	var reply storagerpc.GetListReply
	if err := ls.GetStorageServerConn(key).Call("StorageServer.GetList", args, &reply); err != nil {
		//fmt.Println("GetList finish", key)
		return nil, err
	} else if reply.Status == storagerpc.KeyNotFound {
		//fmt.Println("GetList finish", key)
		return nil, errors.New("GetList operation failed with KeyNotFound")
	} else if reply.Status == storagerpc.WrongServer {
		//fmt.Println("GetList finish", key)
		return nil, errors.New("GetList operation failed with WrongServer")
	} else if reply.Status == storagerpc.ItemNotFound {
		//fmt.Println("GetList finish", key)
		return nil, errors.New("GetList operation failed with ItemNotFound")
	} else {
		if wantLease && reply.Lease.Granted {
			ls.lock.Lock()
			ls.cache[key] = reply.Value
			ls.leases[key] = reply.Lease
			ls.lock.Unlock()
			go ls.DeleteCacheLineWhenTimeout(key)
		}
		//fmt.Println("GetList finish", key)
		return reply.Value, nil
	}
}

func (ls *libstore) RemoveFromList(key, removeItem string) error {

	args := &storagerpc.PutArgs{Key: key, Value: removeItem}
	var reply storagerpc.PutReply

	if err := ls.GetStorageServerConn(key).Call("StorageServer.RemoveFromList", args, &reply); err != nil {
		//fmt.Println("------error when call StorageServer.RemoveFromList")
		//fmt.Println(err)
		//fmt.Println()
		return err
	} else if reply.Status == storagerpc.KeyNotFound {
		return errors.New("RemoveFromList operation failed with KeyNotFound")
	} else if reply.Status == storagerpc.ItemNotFound {
		return errors.New("RemoveFromList operation failed with ItemNotFound")
	} else {
		//fmt.Println("remove from list", key)
		return nil
	}
}

func (ls *libstore) AppendToList(key, newItem string) error {
	//fmt.Println("AppendToList", key)
	args := &storagerpc.PutArgs{Key: key, Value: newItem}
	var reply storagerpc.PutReply

	if err := ls.GetStorageServerConn(key).Call("StorageServer.AppendToList", args, &reply); err != nil {
		return err
	} else if reply.Status == storagerpc.KeyNotFound {
		//fmt.Println("AppendToList finish key not found")
		return errors.New("AppendToList operation failed with KeyNotFound")
	} else if reply.Status == storagerpc.ItemNotFound {
		//fmt.Println("AppendToList finish item not found")
		return errors.New("AppendToList operation failed with ItemNotFound")
	} else if reply.Status == storagerpc.ItemExists {
		//fmt.Println("AppendToList finish item exist")
		return errors.New("AppendToList operation failed with ItemExists")
	} else {
		//fmt.Println("AppendToList finish", key)
		return nil
	}
}

func (ls *libstore) RevokeLease(args *storagerpc.RevokeLeaseArgs, reply *storagerpc.RevokeLeaseReply) error {
	ls.lock.Lock()
	delete(ls.cache, args.Key)
	delete(ls.leases, args.Key)
	ls.lock.Unlock()
	reply.Status = storagerpc.OK
	return nil
}

func (ls *libstore) GetStorageServerConn(key string) *rpc.Client {
	hashVal := StoreHash(key)
	hostPort := ls.storageServerNodes[0].HostPort
	//println("1hostPort", hostPort, len(ls.storageServerNodes))
	upbound := len(ls.storageServerNodes)
	i := 0
	for i < upbound {
		node := ls.storageServerNodes[i]
		if node.NodeID >= hashVal {
			hostPort = node.HostPort
			break
		}
		i++
	}
	if conn, ok := ls.connections[hostPort]; ok {
		return conn
	}
	conn, err := rpc.DialHTTP("tcp", hostPort)
	if err != nil {
		fmt.Println("dial err:", err)
	}
	ls.lock.Lock()
	ls.connections[hostPort] = conn
	ls.lock.Unlock()
	return conn
}
