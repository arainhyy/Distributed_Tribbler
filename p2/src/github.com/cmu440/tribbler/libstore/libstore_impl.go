package libstore

import (
	"errors"

	"github.com/cmu440/tribbler/rpc/storagerpc"
	"net/rpc"
	//"github.com/cmu440/tribbler/rpc/librpc"
)

type libstore struct {
	myHostPort           string
	masterServerHostPort string
	mode                 LeaseMode
	client               *rpc.Client
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
	//lib := new(librpc.RemoteLeaseCallbacks)
	//rpc.RegisterName("LeaseCallbacks", librpc.Wrap(libstore)) // useless in this checkpoint
	return &libstore, nil
}

func (ls *libstore) Get(key string) (string, error) {
	args := &storagerpc.GetArgs{Key:key, WantLease:false}
	var reply storagerpc.GetReply
	if err := ls.client.Call("StorageServer.Get", args, &reply); err != nil {
		return "", err
	} else if reply.Status == storagerpc.KeyNotFound {
		return "", errors.New("GET operation failed with KeyNotFound")
	} else {
		return reply.Value, nil
	}
}

func (ls *libstore) Put(key, value string) error {
	args := &storagerpc.PutArgs{Key:key, Value:value}
	var reply storagerpc.PutReply
	err := ls.client.Call("StorageServer.Put", args, &reply)
	if err != nil {
		return err
	} else if reply.Status == storagerpc.KeyNotFound{
		return errors.New("PUT operation failed with KeyNotFound")
	} else if reply.Status == storagerpc.WrongServer{
		return errors.New("PUT operation failed with WrongServer")
	} else {
		return nil
	}

}

func (ls *libstore) Delete(key string) error {
	args := &storagerpc.DeleteArgs{Key:key}
	var reply storagerpc.DeleteReply
	if err := ls.client.Call("StorageServer.Delete", args, &reply); err != nil {
		return err
	} else if reply.Status == storagerpc.KeyNotFound {
		return errors.New("Delete operation failed with KeyNotFound")
	} else if reply.Status == storagerpc.WrongServer{
		return errors.New("Delete operation failed with WrongServer")
	} else {
		return nil
	}
}

func (ls *libstore) GetList(key string) ([]string, error) {
	args := &storagerpc.GetArgs{Key:key}
	var reply storagerpc.GetListReply
	if err := ls.client.Call("StorageServer.GetList", args, &reply); err != nil {
		return nil, err
	} else if reply.Status == storagerpc.KeyNotFound {
		return nil, errors.New("GetList operation failed with KeyNotFound")
	} else if reply.Status == storagerpc.WrongServer{
		return nil, errors.New("GetList operation failed with WrongServer")
	} else if reply.Status == storagerpc.ItemNotFound{
		return nil, errors.New("GetList operation failed with ItemNotFound")
	} else {
		return reply.Value, nil
	}
}

func (ls *libstore) RemoveFromList(key, removeItem string) error {

	args := &storagerpc.PutArgs{Key:key, Value:removeItem}
	var reply storagerpc.PutReply

	if err := ls.client.Call("StorageServer.RemoveFromList", args, &reply); err != nil {
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
	args := &storagerpc.PutArgs{Key:key, Value:newItem}
	var reply storagerpc.PutReply

	if err := ls.client.Call("StorageServer.AppendToList", args, &reply); err != nil {
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
