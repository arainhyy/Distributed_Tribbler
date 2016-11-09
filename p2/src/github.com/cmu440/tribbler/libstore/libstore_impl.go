package libstore

import (
	"errors"
	"fmt"
	"github.com/cmu440/tribbler/rpc/storagerpc"
	"net/rpc"
	//"github.com/cmu440/tribbler/rpc/librpc"
	"time"
)

type libstore struct {
	myHostPort           string
	masterServerHostPort string
	mode                 LeaseMode
	client               *rpc.Client
	storageServerNodes   []storagerpc.Node
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
		return nil, errors.New("fail to Dail HTTP")
	}
	libstore := libstore{client: cli}
	libstore.myHostPort = myHostPort
	libstore.masterServerHostPort = masterServerHostPort
	libstore.mode = mode
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
		tryTimes++
		time.Sleep(1000 * time.Millisecond)
	}
	libstore.storageServerNodes = reply.Servers
	fmt.Println(reply.Servers[0].HostPort, "hostport")
	//lib := new(librpc.RemoteLeaseCallbacks)
	//rpc.RegisterName("LeaseCallbacks", librpc.Wrap(libstore)) // useless in this checkpoint
	return &libstore, nil
}

func (ls *libstore) Get(key string) (string, error) {
	args := &storagerpc.GetArgs{Key: key, WantLease: false}
	var reply storagerpc.GetReply
	if err := ls.GetStorageServerConn(key).Call("StorageServer.Get", args, &reply); err != nil {
		return "", err
	} else if reply.Status == storagerpc.KeyNotFound {
		return "", errors.New("GET operation failed with KeyNotFound")
	} else {
		return reply.Value, nil
	}
}

func (ls *libstore) Put(key, value string) error {
	args := &storagerpc.PutArgs{Key: key, Value: value}
	var reply storagerpc.PutReply
	err := ls.GetStorageServerConn(key).Call("StorageServer.Put", args, &reply)
	if err != nil {
		return err
	} else if reply.Status == storagerpc.KeyNotFound {
		return errors.New("PUT operation failed with KeyNotFound")
	} else if reply.Status == storagerpc.WrongServer {
		return errors.New("PUT operation failed with WrongServer")
	} else {
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
	args := &storagerpc.GetArgs{Key: key}
	var reply storagerpc.GetListReply
	if err := ls.GetStorageServerConn(key).Call("StorageServer.GetList", args, &reply); err != nil {
		return nil, err
	} else if reply.Status == storagerpc.KeyNotFound {
		return nil, errors.New("GetList operation failed with KeyNotFound")
	} else if reply.Status == storagerpc.WrongServer {
		return nil, errors.New("GetList operation failed with WrongServer")
	} else if reply.Status == storagerpc.ItemNotFound {
		return nil, errors.New("GetList operation failed with ItemNotFound")
	} else {
		return reply.Value, nil
	}
}

func (ls *libstore) RemoveFromList(key, removeItem string) error {

	args := &storagerpc.PutArgs{Key: key, Value: removeItem}
	var reply storagerpc.PutReply

	if err := ls.GetStorageServerConn(key).Call("StorageServer.RemoveFromList", args, &reply); err != nil {
		return err
	} else if reply.Status == storagerpc.KeyNotFound {
		return errors.New("RemoveFromList operation failed with KeyNotFound")
	} else if reply.Status == storagerpc.ItemNotFound {
		return errors.New("RemoveFromList operation failed with ItemNotFound")
	} else {
		return nil
	}
}

func (ls *libstore) AppendToList(key, newItem string) error {
	args := &storagerpc.PutArgs{Key: key, Value: newItem}
	var reply storagerpc.PutReply

	if err := ls.GetStorageServerConn(key).Call("StorageServer.AppendToList", args, &reply); err != nil {
		return err
	} else if reply.Status == storagerpc.KeyNotFound {
		return errors.New("AppendToList operation failed with KeyNotFound")
	} else if reply.Status == storagerpc.ItemNotFound {
		return errors.New("AppendToList operation failed with ItemNotFound")
	} else if reply.Status == storagerpc.ItemExists {
		return errors.New("AppendToList operation failed with ItemExists")
	} else {
		return nil
	}
}

func (ls *libstore) RevokeLease(args *storagerpc.RevokeLeaseArgs, reply *storagerpc.RevokeLeaseReply) error {
	return errors.New("not implemented")
}

func (ls *libstore) GetStorageServerConn(key string) *rpc.Client {
	hashVal := StoreHash(key)
	hostPort := ls.storageServerNodes[0].HostPort
	upbound := len(ls.storageServerNodes)
	i := 1
	for i < upbound {
		node := ls.storageServerNodes[i]
		if node.NodeID >= hashVal {
			hostPort = node.HostPort
			break
		}
		i++
	}
	conn, _ := rpc.DialHTTP("tcp", hostPort)
	return conn
}
