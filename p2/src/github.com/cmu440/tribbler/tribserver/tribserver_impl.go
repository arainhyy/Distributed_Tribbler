package tribserver

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/cmu440/tribbler/libstore"
	"github.com/cmu440/tribbler/rpc/tribrpc"
	"github.com/cmu440/tribbler/util"
	"net"
	"net/http"
	"net/rpc"
	"sort"
	"time"
)

type tribServer struct {
	libstore libstore.Libstore
}

// ByTime implements sort.Interface for []tribrpc.Tribble based on
// the Posted field.
type ByTime []tribrpc.Tribble

func (a ByTime) Len() int           { return len(a) }
func (a ByTime) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByTime) Less(i, j int) bool { return a[j].Posted.After(a[i].Posted) }

// NewTribServer creates, starts and returns a new TribServer. masterServerHostPort
// is the master storage server's host:port and port is this port number on which
// the TribServer should listen. A non-nil error should be returned if the TribServer
// could not be started.
//
// For hints on how to properly setup RPC, see the rpc/tribrpc package.
func NewTribServer(masterServerHostPort, myHostPort string) (TribServer, error) {
	libstore, err := libstore.NewLibstore(masterServerHostPort, myHostPort, libstore.Normal)
	tribServer := &tribServer{
		libstore: libstore,
	}
	listener, err := net.Listen("tcp", myHostPort)
	err = rpc.RegisterName("TribServer", tribrpc.Wrap(tribServer))
	if err != nil {
		return nil, errors.New("Fail to register tribserver.")
	}
	rpc.HandleHTTP()
	go http.Serve(listener, nil)
	return tribServer, nil
}

func (ts *tribServer) CreateUser(args *tribrpc.CreateUserArgs, reply *tribrpc.CreateUserReply) error {
	id := util.FormatUserKey(args.UserID)
	_, err := ts.libstore.Get(id)
	if err == nil {
		reply.Status = tribrpc.Exists
		return nil
	}
	ts.libstore.Put(id, args.UserID)
	reply.Status = tribrpc.OK
	return nil
}

func (ts *tribServer) AddSubscription(args *tribrpc.SubscriptionArgs, reply *tribrpc.SubscriptionReply) error {
	id := util.FormatUserKey(args.UserID)
	_, err := ts.libstore.Get(id)
	// If this user does not exist, reply directly.
	if err != nil {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}
	// If target user does not exist, reply directly.
	targetID := util.FormatUserKey(args.TargetUserID)
	_, err = ts.libstore.Get(targetID)
	if err != nil {
		reply.Status = tribrpc.NoSuchTargetUser
		return nil
	}
	// If target user has been substribed by this user, reply directly.
	err = ts.libstore.AppendToList(util.FormatSubListKey(args.UserID), args.TargetUserID)
	if err != nil {
		reply.Status = tribrpc.Exists
		return nil
	}
	reply.Status = tribrpc.OK
	return nil
}

func (ts *tribServer) RemoveSubscription(args *tribrpc.SubscriptionArgs, reply *tribrpc.SubscriptionReply) error {
	id := util.FormatUserKey(args.UserID)
	_, err := ts.libstore.Get(id)
	// If this user does not exist, reply directly.
	if err != nil {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}
	// If target user does not exist, reply directly.
	targetID := util.FormatUserKey(args.TargetUserID)
	_, err = ts.libstore.Get(targetID)
	if err != nil {
		reply.Status = tribrpc.NoSuchTargetUser
		return nil
	}
	// If target user is not substribed by this user, reply directly.
	err = ts.libstore.RemoveFromList(util.FormatSubListKey(args.UserID), args.TargetUserID)
	if err != nil {
		fmt.Println("Target user", args.TargetUserID, "is not subscribed by this user", args.UserID)
		reply.Status = tribrpc.NoSuchTargetUser
		return nil
	}
	reply.Status = tribrpc.OK
	return nil
}

func (ts *tribServer) GetFriends(args *tribrpc.GetFriendsArgs, reply *tribrpc.GetFriendsReply) error {
	id := util.FormatUserKey(args.UserID)
	_, err := ts.libstore.Get(id)
	// If this user does not exist, reply directly.
	if err != nil {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}
	list, err := ts.libstore.GetList(util.FormatSubListKey(args.UserID))
	if err != nil {
		fmt.Println("Fail to get friend list of a user: ", id)
		return nil
	}
	reply.Status = tribrpc.OK
	reply.UserIDs = list
	return nil
}

func (ts *tribServer) PostTribble(args *tribrpc.PostTribbleArgs, reply *tribrpc.PostTribbleReply) error {
	id := util.FormatUserKey(args.UserID)
	_, err := ts.libstore.Get(id)
	// If this user does not exist, reply directly.
	if err != nil {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}
	timestamp := time.Now().UTC().UnixNano()
	postID := util.FormatPostKey(args.UserID, timestamp)
	_, err = ts.libstore.Get(postID)
	// If postID has existed, keey trying with new timestamp to create new postID.
	for err == nil {
		timestamp = time.Now().UTC().UnixNano()
		postID = util.FormatPostKey(args.UserID, timestamp)
		_, err = ts.libstore.Get(postID)
	}
	// Create new tribble.
	tribble := &tribrpc.Tribble{
		UserID:   args.UserID,
		Posted:   time.Now(),
		Contents: args.Contents,
	}
	marshalledTribble, err := json.Marshal(tribble)
	if err != nil {
		fmt.Println("Marshal error inside post tribble.")
	}
	err = ts.libstore.Put(postID, string(marshalledTribble))
	if err != nil {
		fmt.Println("Fail to put marshalled tribble into libstore.")
		return nil
	}
	// Put ID of this tribble to tribble list of this user.
	tribListUserID := util.FormatTribListKey(args.UserID)
	err = ts.libstore.AppendToList(tribListUserID, postID)
	if err != nil {
		fmt.Println("Fail to add new posted tribble to user's list")
		return nil
	}
	reply.Status = tribrpc.OK
	reply.PostKey = postID
	return nil
}

func (ts *tribServer) DeleteTribble(args *tribrpc.DeleteTribbleArgs, reply *tribrpc.DeleteTribbleReply) error {
	id := util.FormatUserKey(args.UserID)
	_, err := ts.libstore.Get(id)
	// If this user does not exist, reply directly.
	if err != nil {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}
	_, err = ts.libstore.Get(args.PostKey)
	// If this post does not exist, reply directly.
	if err != nil {
		reply.Status = tribrpc.NoSuchPost
		return nil
	}
	// Delete post entry.
	ts.libstore.Delete(args.PostKey)
	// Delete post from user post list.
	err = ts.libstore.RemoveFromList(util.FormatTribListKey(args.UserID), args.PostKey)
	if err != nil {
		fmt.Println("Fail to remove tribble", args.PostKey, "from user post list")
		return nil
	}
	reply.Status = tribrpc.OK
	return nil
}

func (ts *tribServer) GetTribbles(args *tribrpc.GetTribblesArgs, reply *tribrpc.GetTribblesReply) error {
	id := util.FormatUserKey(args.UserID)
	_, err := ts.libstore.Get(id)
	// If this user does not exist, reply directly.
	if err != nil {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}
	tribbleList, err := ts.libstore.GetList(util.FormatTribListKey(args.UserID))
	if err != nil {
		reply.Status = tribrpc.OK
		fmt.Println("Fail to get tribble list of user", id)
		return nil
	}
	// Put all tribble of this user into list then sort them by Posted field.
	allTribbleList := make([]tribrpc.Tribble, len(tribbleList))
	for i := 0; i < len(tribbleList); i++ {
		tribble, _ := ts.libstore.Get(tribbleList[i])
		json.Unmarshal([]byte(tribble), &allTribbleList[i])
	}
	sort.Sort(ByTime(allTribbleList))
	var num int
	if 100 < len(allTribbleList) {
		num = 100
	} else {
		num = len(allTribbleList)
	}
	// return the most recent 100 or less posts.
	returnList := make([]tribrpc.Tribble, num)
	for i := 0; i < num; i++ {
		returnList[i] = allTribbleList[num-i-1]
	}
	reply.Status = tribrpc.OK
	reply.Tribbles = returnList
	return nil
}

func (ts *tribServer) GetTopTribbleByUserID(userID string) []tribrpc.Tribble {
	id := util.FormatTribListKey(userID)
	tribList, err := ts.libstore.GetList(id)
	if err != nil {
		fmt.Println("Fail to get top tribbles by userID", userID)
		return nil
	}
	// Get tribble from libstore by tribbleID in tribble list of this user. Put it into return list.
	returnList := make([]tribrpc.Tribble, len(tribList))
	for i := 0; i < len(tribList); i++ {
		bytetribble, _ := ts.libstore.Get(tribList[i])
		var tribble tribrpc.Tribble
		json.Unmarshal([]byte(bytetribble), &tribble)
		returnList[i] = tribble
	}
	return returnList
}

func (ts *tribServer) GetTribblesBySubscription(args *tribrpc.GetTribblesArgs, reply *tribrpc.GetTribblesReply) error {
	id := util.FormatUserKey(args.UserID)
	_, err := ts.libstore.Get(id)
	// If this user does not exist, reply directly.
	if err != nil {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}

	subUserID := util.FormatSubListKey(args.UserID)
	subUserList, err := ts.libstore.GetList(subUserID)
	if err != nil {
		fmt.Println("Fail to get subscription list by user ID, ", subUserID)
		return nil
	}
	var allTribbleList []tribrpc.Tribble
	// Get tribbles of all subscribed users.
	for i := 0; i < len(subUserList); i++ {
		list := ts.GetTopTribbleByUserID(subUserList[i])
		allTribbleList = append(allTribbleList, list...)
	}
	sort.Sort(ByTime(allTribbleList))
	var num int
	if 100 < len(allTribbleList) {
		num = 100
	} else {
		num = len(allTribbleList)
	}
	// return the most recent 100 or less posts.
	returnList := make([]tribrpc.Tribble, num)
	for i := 0; i < num; i++ {
		returnList[i] = allTribbleList[num-i-1]
	}
	reply.Status = tribrpc.OK
	reply.Tribbles = returnList
	return nil
}
