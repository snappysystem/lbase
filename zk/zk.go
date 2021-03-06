/*
Copyright (c) 2015, snappysystem
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:

* Redistributions of source code must retain the above copyright notice, this
  list of conditions and the following disclaimer.

* Redistributions in binary form must reproduce the above copyright notice,
  this list of conditions and the following disclaimer in the documentation
  and/or other materials provided with the distribution.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/
package zk

/*
#cgo CFLAGS: -I/usr/include
#cgo LDFLAGS: -lzookeeper_mt

#include <zookeeper/zookeeper.h>

void my_global_watcher(
  zhandle_t *zh,
  int type,
  int state,
  const char* path,
  void *ctx) {
}

extern void GoWatcher(int type, int state, void* path, void* ctx);

void my_watcher(
  zhandle_t *zh,
  int type,
  int state,
  const char* path,
  void *ctx) {
  GoWatcher(type, state, (void*)path, ctx);
}

extern void GoVoidCompletion(int rc, void* data);

void my_void_completion(int rc, const void *data) {
  GoVoidCompletion(rc, (void*)data);
}

extern void GoStatCompletion(int rc, void* stat, void* data);

void my_stat_completion(int rc, const struct Stat *stat, const void *data) {
  GoStatCompletion(rc, (void*)stat, (void*)data);
}

extern void GoDataCompletion(
  int rc, void* value, int value_len, void* stat, void* data);

void my_data_completion(int rc, const char *value, int value_len,
       const struct Stat *stat, const void *data) {
  GoDataCompletion(rc, (void*)value, value_len, (void*)stat, (void*)data);
}

extern void GoStringsCompletion(int rc, void* strings, void* data);

void my_strings_completion(int rc,
       const struct String_vector *strings, const void *data) {
  GoStringsCompletion(rc, (void*)strings, (void*)data);
}

extern void GoStringsStatCompletion(int rc, void* strings, void* stat, void* data);

void my_strings_stat_completion(int rc, const struct String_vector *strings,
  const struct Stat *stat, const void *data) {
  GoStringsStatCompletion(rc, (void*)strings, (void*)stat, (void*)data);
}

extern void GoStringCompletion(int rc, void* value, void* data);

void my_string_completion(int rc, const char *value, const void *data) {
  GoStringCompletion(rc, (void*)value, (void*)data);
}

extern void GoACLCompletion(int rc, void* aclVec, void* stat, void* data);

void my_acl_completion(int rc, struct ACL_vector *acl,
       struct Stat *stat, const void *data) {
  GoACLCompletion(rc, (void*)acl, (void*)stat, (void*)data);
}
*/
import "C"

import (
	"unsafe"
	"reflect"
)

// Zookeeper return code.
const (
	ZOK = C.ZOK

	/** System and server-side errors.
	 * This is never thrown by the server, it shouldn't be used other than
	 * to indicate a range. Specifically error codes greater than this
	 * value, but lesser than {@link #ZAPIERROR}, are system errors. */
	ZSYSTEMERROR = C.ZSYSTEMERROR
	ZRUNTIMEINCONSISTENCY = C.ZRUNTIMEINCONSISTENCY
	ZDATAINCONSISTENCY = C.ZDATAINCONSISTENCY
	ZCONNECTIONLOSS = C.ZCONNECTIONLOSS
	ZMARSHALLINGERROR = C.ZMARSHALLINGERROR
	ZUNIMPLEMENTED = C.ZUNIMPLEMENTED
	ZOPERATIONTIMEOUT = C.ZOPERATIONTIMEOUT
	ZBADARGUMENTS = C.ZBADARGUMENTS
	ZINVALIDSTATE = C.ZINVALIDSTATE

	/** API errors.
	 * This is never thrown by the server, it shouldn't be used other than
	 * to indicate a range. Specifically error codes greater than this
	 * value are API errors (while values less than this indicate a 
	 * {@link #ZSYSTEMERROR}).
	 */
	ZAPIERROR = C.ZAPIERROR
	ZNONODE = C.ZNONODE
	ZNOAUTH = C.ZNOAUTH
	ZBADVERSION = C.ZBADVERSION
	ZNOCHILDRENFOREPHEMERALS = C.ZNOCHILDRENFOREPHEMERALS
	ZNODEEXISTS = C.ZNODEEXISTS
	ZNOTEMPTY = C.ZNOTEMPTY
	ZSESSIONEXPIRED = C.ZSESSIONEXPIRED
	ZINVALIDCALLBACK = C.ZINVALIDCALLBACK
	ZINVALIDACL = C.ZINVALIDACL
	ZAUTHFAILED = C.ZAUTHFAILED
	ZCLOSING = C.ZCLOSING
	ZNOTHING = C.ZNOTHING
	ZSESSIONMOVED = C.ZSESSIONMOVED
)

/**
 * @name ACL Consts
 */
var (
	ZOO_PERM_READ = C.ZOO_PERM_READ
	ZOO_PERM_WRITE = C.ZOO_PERM_WRITE
	ZOO_PERM_CREATE = C.ZOO_PERM_CREATE
	ZOO_PERM_DELETE = C.ZOO_PERM_DELETE
	ZOO_PERM_ADMIN = C.ZOO_PERM_ADMIN
	ZOO_PERM_ALL = C.ZOO_PERM_ALL
)

/** Zookeeper ACL constants */
var (
	/** This is a completely open ACL*/
	ZOO_OPEN_ACLS = NewACLs(&C.ZOO_OPEN_ACL_UNSAFE)
	/** This ACL gives the world the ability to read. */
	ZOO_READ_ACLS = NewACLs(&C.ZOO_READ_ACL_UNSAFE)
	/** This ACL gives the creators authentication id's all permissions. */
	ZOO_CREATOR_ALL_ACLS = NewACLs(&C.ZOO_CREATOR_ALL_ACL)
)

/**
 * @name Create Flags
 * 
 * These flags are used by zoo_create to affect node create. They may
 * be ORed together to combine effects.
 */
var (
	ZOO_EPHEMERAL = C.ZOO_EPHEMERAL
	ZOO_SEQUENCE = C.ZOO_SEQUENCE
)

/**
 * @name State Consts
 * These constants represent the states of a zookeeper connection. They are
 * possible parameters of the watcher callback.
 */
var (
	ZOO_EXPIRED_SESSION_STATE = int(C.ZOO_EXPIRED_SESSION_STATE)
	ZOO_AUTH_FAILED_STATE = int(C.ZOO_AUTH_FAILED_STATE)
	ZOO_CONNECTING_STATE = int(C.ZOO_CONNECTING_STATE)
	ZOO_ASSOCIATING_STATE = int(C.ZOO_ASSOCIATING_STATE)
	ZOO_CONNECTED_STATE = int(C.ZOO_CONNECTED_STATE)
)

/**
 * @name Watch Types
 * These constants indicate the event that caused the watch event. They are
 * possible values of the first parameter of the watcher callback.
 */
var (
	/**
	 * \brief a node has been created.
	 * 
	 * This is only generated by watches on non-existent nodes. These watches
	 * are set using \ref zoo_exists.
	 */
	ZOO_CREATED_EVENT = int(C.ZOO_CREATED_EVENT)
	/**
	 * \brief a node has been deleted.
	 * 
	 * This is only generated by watches on nodes. These watches
	 * are set using \ref zoo_exists and \ref zoo_get.
	 */
	ZOO_DELETED_EVENT = int(C.ZOO_DELETED_EVENT)
	/**
	 * \brief a node has changed.
	 * 
	 * This is only generated by watches on nodes. These watches
	 * are set using \ref zoo_exists and \ref zoo_get.
	 */
	ZOO_CHANGED_EVENT = int(C.ZOO_CHANGED_EVENT)
	/**
	 * \brief a change as occurred in the list of children.
	 * 
	 * This is only generated by watches on the child list of a node. These watches
	 * are set using \ref zoo_get_children or \ref zoo_get_children2.
	 */
	ZOO_CHILD_EVENT = int(C.ZOO_CHILD_EVENT)
	/**
	 * \brief a session has been lost.
	 * 
	 * This is generated when a client loses contact or reconnects with a server.
	 */
	ZOO_SESSION_EVENT = int(C.ZOO_SESSION_EVENT)
	/**
	 * \brief a watch has been removed.
	 * 
	 * This is generated when the server for some reason, probably a resource
	 * constraint, will no longer watch a node for a client.
	 */
	ZOO_NOTWATCHING_EVENT = int(C.ZOO_NOTWATCHING_EVENT)
)

type ACL struct {
	Perms int
	Scheme string
	Id string
}

// Convert a C ACL vector into go ACL slice.
func NewACLs(aclCVec *C.struct_ACL_vector) []ACL {
	goACLs := make([]ACL, 0, aclCVec.count)

	// Simulate a go slice.
	ppvHdr := reflect.SliceHeader{
		Data: uintptr(unsafe.Pointer(aclCVec.data)),
		Len: int(aclCVec.count),
		Cap: int(aclCVec.count),
	}
	goSlice := *(*[]C.struct_ACL)(unsafe.Pointer(&ppvHdr))

	for _,s := range goSlice {
		acl := ACL{
			Perms: int(s.perms),
			Scheme: C.GoString(s.id.scheme),
			Id: C.GoString(s.id.id),
		}
		goACLs = append(goACLs, acl)
	}

	return goACLs
}

type Watcher struct {
	Type int
	State int
	Path string
}

type StatResult struct {
	rc C.int
	stat C.struct_Stat
}

func (sr *StatResult) GetRc() int {
	return int(sr.rc)
}

func (sr *StatResult) GetCtime() int64 {
	return int64(sr.stat.ctime)
}

func (sr *StatResult) GetMtime() int64 {
	return int64(sr.stat.mtime)
}

func (sr *StatResult) GetVersion() int32 {
	return int32(sr.stat.version)
}

func (sr *StatResult) GetCversion() int32 {
	return int32(sr.stat.cversion)
}

func (sr *StatResult) GetAversion() int32 {
	return int32(sr.stat.aversion)
}

func (sr *StatResult) GetEphemeralOwner() int64 {
	return int64(sr.stat.ephemeralOwner)
}

func (sr *StatResult) GetDataLength() int32 {
	return int32(sr.stat.dataLength)
}

func (sr *StatResult) GetNumChildren() int32 {
	return int32(sr.stat.numChildren)
}

type DataResult struct {
	rc C.int
	data []byte
	stat C.struct_Stat
}

func (sr *DataResult) GetData() []byte {
	return sr.data
}

func (sr *DataResult) GetRc() int {
	return int(sr.rc)
}

func (sr *DataResult) GetCtime() int64 {
	return int64(sr.stat.ctime)
}

func (sr *DataResult) GetMtime() int64 {
	return int64(sr.stat.mtime)
}

func (sr *DataResult) GetVersion() int32 {
	return int32(sr.stat.version)
}

func (sr *DataResult) GetCversion() int32 {
	return int32(sr.stat.cversion)
}

func (sr *DataResult) GetAversion() int32 {
	return int32(sr.stat.aversion)
}

func (sr *DataResult) GetEphemeralOwner() int64 {
	return int64(sr.stat.ephemeralOwner)
}

func (sr *DataResult) GetDataLength() int32 {
	return int32(sr.stat.dataLength)
}

func (sr *DataResult) GetNumChildren() int32 {
	return int32(sr.stat.numChildren)
}

type StringsResult struct {
	rc C.int
	strings []string
}

func (sr *StringsResult) GetStrings() []string {
	return sr.strings
}

func (sr *StringsResult) GetRc() int {
	return int(sr.rc)
}

type StringsStatResult struct {
	rc C.int
	strings []string
	stat C.struct_Stat
}

func (sr *StringsStatResult) GetStrings() []string {
	return sr.strings
}

func (sr *StringsStatResult) GetRc() int {
	return int(sr.rc)
}

func (sr *StringsStatResult) GetCtime() int64 {
	return int64(sr.stat.ctime)
}

func (sr *StringsStatResult) GetMtime() int64 {
	return int64(sr.stat.mtime)
}

func (sr *StringsStatResult) GetVersion() int32 {
	return int32(sr.stat.version)
}

func (sr *StringsStatResult) GetCversion() int32 {
	return int32(sr.stat.cversion)
}

func (sr *StringsStatResult) GetAversion() int32 {
	return int32(sr.stat.aversion)
}

func (sr *StringsStatResult) GetEphemeralOwner() int64 {
	return int64(sr.stat.ephemeralOwner)
}

func (sr *StringsStatResult) GetDataLength() int32 {
	return int32(sr.stat.dataLength)
}

type StringResult struct {
	rc C.int
	str string
}

func (sr *StringResult) GetString() string {
	return sr.str
}

func (sr *StringResult) GetRc() int {
	return int(sr.rc)
}

type ACLResult struct {
	rc C.int
	acls []ACL
	stat C.struct_Stat
}

func (sr *ACLResult) GetACLs() []ACL {
	return sr.acls
}

func (sr *ACLResult) GetRc() int {
	return int(sr.rc)
}

func (sr *ACLResult) GetCtime() int64 {
	return int64(sr.stat.ctime)
}

func (sr *ACLResult) GetMtime() int64 {
	return int64(sr.stat.mtime)
}

func (sr *ACLResult) GetVersion() int32 {
	return int32(sr.stat.version)
}

func (sr *ACLResult) GetCversion() int32 {
	return int32(sr.stat.cversion)
}

func (sr *ACLResult) GetAversion() int32 {
	return int32(sr.stat.aversion)
}

func (sr *ACLResult) GetEphemeralOwner() int64 {
	return int64(sr.stat.ephemeralOwner)
}

func (sr *ACLResult) GetDataLength() int32 {
	return int32(sr.stat.dataLength)
}


type ZkID struct {
	id C.clientid_t
}

type ZHandle struct {
	handle *C.zhandle_t
}

/**
 * \brief create a new zookeeper handle to communicate with server.
 * 
 * This method creates a new handle and a zookeeper session that corresponds
 * to that handle. Session establishment is asynchronous, meaning that the
 * session should not be considered established until (and unless) an
 * event of state ZOO_CONNECTED_STATE is received.
 * \param host comma separated host:port pairs, each corresponding to a zk
 *   server. e.g. "127.0.0.1:3000,127.0.0.1:3001,127.0.0.1:3002"
 * \param recvTimeout The current implementation requires that the timeout
 * be a minimum of 2 times the tickTime (as set in the server configuration)
 * and a maximum of 20 times the tickTime.
 * \param clientid the id of a previously established session that this
 *   client will be reconnecting to. Pass 0 if not reconnecting to a previous
 *   session. Clients can access the session id of an established, valid,
 *   connection by calling \ref zoo_client_id. If the session corresponding to
 *   the specified clientid has expired, or if the clientid is invalid for 
 *   any reason, the returned zhandle_t will be invalid -- the zhandle_t 
 *   state will indicate the reason for failure (typically
 *   ZOO_EXPIRED_SESSION_STATE).
 * \return a pointer to the opaque zhandle structure. If it fails to create 
 * a new zhandle the function returns NULL and the errno variable 
 * indicates the reason.
 */
func NewZHandle(hosts string, recvTimeout int, id *ZkID) (h ZHandle, ok bool) {
	// Suppress most of log messages.
	C.zoo_set_debug_level(C.ZOO_LOG_LEVEL_ERROR)

	chosts := C.CString(hosts)
	defer C.free(unsafe.Pointer(chosts))

	var cid *C.clientid_t
	if id != nil {
		cid = &id.id
	}

	handle, err := C.zookeeper_init(
		chosts, (C.watcher_fn)(C.my_global_watcher), C.int(recvTimeout), cid, nil, 0)

	if err != nil {
		ok = false
	} else {
		ok = true
		h = ZHandle{handle: handle}
	}
	return
}

/**
 * \brief get the state of the zookeeper connection.
 * 
 * The return value will be one of the \ref State Consts.
 */
func (zh *ZHandle) GetState() int {
	return int(C.zoo_state(zh.handle))
}

/**
 * \brief checks the existence of a node in zookeeper.
 * 
 */
func (zh *ZHandle) Exists(path string) StatResult {
	cpath := C.CString(path)
	defer C.free(unsafe.Pointer(cpath))

	res := make(chan StatResult, 1)
	rc,err := C.zoo_awexists(
		zh.handle,
		cpath,
		nil,
		nil,
		C.stat_completion_t(C.my_stat_completion),
		unsafe.Pointer(&res))

	var ret StatResult
	if err != nil {
		ret.rc = rc
	} else {
		ret = <-res
	}

	return ret
}

/**
 * \brief checks the existence of a node in zookeeper.
 * 
 */
func (zh *ZHandle) ExistsW(path string) (StatResult, chan Watcher) {
	cpath := C.CString(path)
	defer C.free(unsafe.Pointer(cpath))

	res1 := make(chan StatResult, 1)
	res2 := make(chan Watcher, 1)

	rc,err := C.zoo_awexists(
		zh.handle,
		cpath,
		C.watcher_fn(C.my_watcher),
		unsafe.Pointer(&res2),
		C.stat_completion_t(C.my_stat_completion),
		unsafe.Pointer(&res1))

	var ret StatResult
	if err != nil {
		ret.rc = rc
	} else {
		ret = <-res1
	}

	return ret, res2
}

/**
 * \brief gets the data associated with a node.
 */
func (zh *ZHandle) Get(path string) DataResult {
	cpath := C.CString(path)
	defer C.free(unsafe.Pointer(cpath))

	res := make(chan DataResult, 1)
	rc,err := C.zoo_awget(
		zh.handle,
		cpath,
		nil,
		nil,
		C.data_completion_t(C.my_data_completion),
		unsafe.Pointer(&res))

	var ret DataResult
	if err != nil {
		ret.rc = rc
	} else {
		ret = <-res
	}

	return ret
}

/**
 * \brief gets the data associated with a node.
 */
func (zh *ZHandle) GetW(path string) (DataResult, chan Watcher) {
	cpath := C.CString(path)
	defer C.free(unsafe.Pointer(cpath))

	res1 := make(chan DataResult, 1)
	res2 := make(chan Watcher, 1)

	rc,err := C.zoo_awget(
		zh.handle,
		cpath,
		C.watcher_fn(C.my_watcher),
		unsafe.Pointer(&res2),
		C.data_completion_t(C.my_data_completion),
		unsafe.Pointer(&res1))

	var ret DataResult
	if err != nil {
		ret.rc = rc
	} else {
		ret = <-res1
	}

	return ret, res2
}

/**
 * \brief sets the data associated with a node.
 * 
 * \param path the name of the node. Expressed as a file name with slashes 
 * separating ancestors of the node.
 * \param buffer the buffer holding data to be written to the node.
 * \param version the expected version of the node. The function will fail if 
 * the actual version of the node does not match the expected version. If -1 is 
 * used the version check will not take place. * completion: If null, 
 * the function will execute synchronously. Otherwise, the function will return 
 * immediately and invoke the completion routine when the request completes.
 * \return ZOK on success or one of the following errcodes on failure:
 * ZBADARGUMENTS - invalid input parameters
 * ZINVALIDSTATE - zhandle state is either ZOO_SESSION_EXPIRED_STATE or ZOO_AUTH_FAILED_STATE
 * ZMARSHALLINGERROR - failed to marshall a request; possibly, out of memory
 */
func (zh *ZHandle) Set(path string, buffer []byte, version int) StatResult {
	cpath := C.CString(path)
	defer C.free(unsafe.Pointer(cpath))

	res := make(chan StatResult, 1)
	rc,err := C.zoo_aset(
		zh.handle,
		cpath,
		(*C.char)(unsafe.Pointer(&buffer[0])),
		C.int(len(buffer)),
		C.int(version),
		C.stat_completion_t(C.my_stat_completion),
		unsafe.Pointer(&res))

	var ret StatResult
	if err != nil {
		ret.rc = rc
	} else {
		ret = <-res
	}

	return ret
}

/**
 * \brief lists the children of a node.
 * 
 * \param path the name of the node. Expressed as a file name with slashes 
 * separating ancestors of the node.
 */
func (zh *ZHandle) GetChildren(path string) StringsResult {
	cpath := C.CString(path)
	defer C.free(unsafe.Pointer(cpath))

	res := make(chan StringsResult, 1)
	rc,err := C.zoo_awget_children(
		zh.handle,
		cpath,
		nil,
		nil,
		C.strings_completion_t(C.my_strings_completion),
		unsafe.Pointer(&res))

	var ret StringsResult
	if err != nil {
		ret.rc = rc
	} else {
		ret = <-res
	}

	return ret
}

/**
 * \brief lists the children of a node.
 * 
 * \param path the name of the node. Expressed as a file name with slashes 
 * separating ancestors of the node.
 */
func (zh *ZHandle) GetChildrenW(path string) (StringsResult, chan Watcher) {
	cpath := C.CString(path)
	defer C.free(unsafe.Pointer(cpath))

	res1 := make(chan StringsResult, 1)
	res2 := make(chan Watcher, 1)

	rc,err := C.zoo_awget_children(
		zh.handle,
		cpath,
		C.watcher_fn(C.my_watcher),
		unsafe.Pointer(&res2),
		C.strings_completion_t(C.my_strings_completion),
		unsafe.Pointer(&res1))

	var ret StringsResult
	if err != nil {
		ret.rc = rc
	} else {
		ret = <-res1
	}

	return ret, res2
}

/**
 * \brief lists the children of a node, and get the parent stat.
 * 
 * \param path the name of the node. Expressed as a file name with slashes 
 * separating ancestors of the node.
 */
func (zh *ZHandle) GetChildrenAndStat(path string) StringsStatResult {
	cpath := C.CString(path)
	defer C.free(unsafe.Pointer(cpath))

	res := make(chan StringsStatResult, 1)
	rc,err := C.zoo_awget_children2(
		zh.handle,
		cpath,
		nil,
		nil,
		C.strings_stat_completion_t(C.my_strings_stat_completion),
		unsafe.Pointer(&res))

	var ret StringsStatResult
	if err != nil {
		ret.rc = rc
	} else {
		ret = <-res
	}

	return ret
}

func (zh *ZHandle) GetChildrenAndStatW(path string) (StringsStatResult, chan Watcher) {
	cpath := C.CString(path)
	defer C.free(unsafe.Pointer(cpath))

	res1 := make(chan StringsStatResult, 1)
	res2 := make(chan Watcher, 1)

	rc,err := C.zoo_awget_children2(
		zh.handle,
		cpath,
		C.watcher_fn(C.my_watcher),
		unsafe.Pointer(&res2),
		C.strings_stat_completion_t(C.my_strings_stat_completion),
		unsafe.Pointer(&res1))

	var ret StringsStatResult
	if err != nil {
		ret.rc = rc
	} else {
		ret = <-res1
	}

	return ret, res2
}

/**
 * \brief Flush leader channel.
 *
 * \param path the name of the node. Expressed as a file name with slashes
 * separating ancestors of the node.
 * \return ZOK on success or one of the following errcodes on failure:
 * ZBADARGUMENTS - invalid input parameters
 * ZINVALIDSTATE - zhandle state is either ZOO_SESSION_EXPIRED_STATE or ZOO_AUTH_FAILED_STATE
 * ZMARSHALLINGERROR - failed to marshall a request; possibly, out of memory
 */
func (zh *ZHandle) Sync(path string) StringResult {
	cpath := C.CString(path)
	defer C.free(unsafe.Pointer(cpath))

	res := make(chan StringResult, 1)
	rc,err := C.zoo_async(
		zh.handle,
		cpath,
		C.string_completion_t(C.my_string_completion),
		unsafe.Pointer(&res))

	var ret StringResult
	if err != nil {
		ret.rc = rc
	} else {
		ret = <-res
	}

	return ret
}

/**
 * \brief gets the acl associated with a node.
 * 
 * \param path the name of the node. Expressed as a file name with slashes 
 * separating ancestors of the node.
 * \param completion the routine to invoke when the request completes. The completion
 * will be triggered with one of the following codes passed in as the rc argument:
 * ZOK operation completed successfully
 * ZNONODE the node does not exist.
 * ZNOAUTH the client does not have permission.
 * \param data the data that will be passed to the completion routine when 
 * the function completes.
 * \return ZOK on success or one of the following errcodes on failure:
 * ZBADARGUMENTS - invalid input parameters
 * ZINVALIDSTATE - zhandle state is either ZOO_SESSION_EXPIRED_STATE or ZOO_AUTH_FAILED_STATE
 * ZMARSHALLINGERROR - failed to marshall a request; possibly, out of memory
 */
func (zh *ZHandle) GetACL(path string) ACLResult {
	cpath := C.CString(path)
	defer C.free(unsafe.Pointer(cpath))

	res := make(chan ACLResult, 1)
	rc,err := C.zoo_aget_acl(
		zh.handle,
		cpath,
		C.acl_completion_t(C.my_acl_completion),
		unsafe.Pointer(&res))

	var ret ACLResult
	if err != nil {
		ret.rc = rc
	} else {
		ret = <-res
	}

	return ret
}

/**
 * \brief sets the acl associated with a node synchronously.
 * 
 * \param zh the zookeeper handle obtained by a call to \ref zookeeper_init
 * \param path the name of the node. Expressed as a file name with slashes 
 * separating ancestors of the node.
 * \param version the expected version of the path.
 * \param acl the acl to be set on the path. 
 * \return the return code for the function call.
 * ZOK operation completed successfully
 * ZNONODE the node does not exist.
 * ZNOAUTH the client does not have permission.
 * ZINVALIDACL invalid ACL specified
 * ZBADVERSION expected version does not match actual version.
 * ZBADARGUMENTS - invalid input parameters
 * ZINVALIDSTATE - zhandle state is either ZOO_SESSION_EXPIRED_STATE or ZOO_AUTH_FAILED_STATE
 * ZMARSHALLINGERROR - failed to marshall a request; possibly, out of memory
 */
func (zh *ZHandle) SetACL(path string, version int, goACLs []ACL) int {
	cpath := C.CString(path)
	defer C.free(unsafe.Pointer(cpath))

	// Convert ACL from go struct to C struct.
	cACLs := make([]C.struct_ACL, len(goACLs))
	tmps := make([][]byte, 0, 2 * len(goACLs))

	for i := 0; i < len(goACLs); i++ {
		schemeBytes := []byte(goACLs[i].Scheme)
		idBytes := []byte(goACLs[i].Id)
		tmps = append(tmps, schemeBytes, idBytes)
		cACLs[i].perms = C.int32_t(goACLs[i].Perms)
		cACLs[i].id.scheme = (*C.char)(unsafe.Pointer(&schemeBytes[0]))
		cACLs[i].id.id = (*C.char)(unsafe.Pointer(&idBytes[0]))
	}

	var cVector C.struct_ACL_vector

	cVector.count = C.int32_t(len(goACLs))
	cVector.data = &cACLs[0]

	res := make(chan int, 1)

	rc,err := C.zoo_aset_acl(
		zh.handle,
		cpath,
		C.int(version),
		&cVector,
		C.void_completion_t(C.my_void_completion),
		unsafe.Pointer(&res))

	var ret int
	if err != nil {
		ret = int(rc)
	} else {
		ret = <-res
	}

	return ret
}

/**
 * \brief create a node.
 * 
 * This method will create a node in ZooKeeper. A node can only be created if
 * it does not already exists. The Create Flags affect the creation of nodes.
 * If ZOO_EPHEMERAL flag is set, the node will automatically get removed if the
 * client session goes away. If the ZOO_SEQUENCE flag is set, a unique
 * monotonically increasing sequence number is appended to the path name. The
 * sequence number is always fixed length of 10 digits, 0 padded.
 * 
 * \param path The name of the node. Expressed as a file name with slashes 
 * separating ancestors of the node.
 * \param value The data to be stored in the node.
 * \param acl The initial ACL of the node. The ACL must not be null or empty.
 * \param flags this parameter can be set to 0 for normal create or an OR
 *    of the Create Flags
 * \param completion the routine to invoke when the request completes. The completion
 * will be triggered with one of the following codes passed in as the rc argument:
 * ZOK operation completed successfully
 * ZNONODE the parent node does not exist.
 * ZNODEEXISTS the node already exists
 * ZNOAUTH the client does not have permission.
 * ZNOCHILDRENFOREPHEMERALS cannot create children of ephemeral nodes.
 * \return ZOK on success or one of the following errcodes on failure:
 * ZBADARGUMENTS - invalid input parameters
 * ZINVALIDSTATE - zhandle state is either ZOO_SESSION_EXPIRED_STATE or ZOO_AUTH_FAILED_STATE
 * ZMARSHALLINGERROR - failed to marshall a request; possibly, out of memory
 */
func (zh *ZHandle) Create(path, value string, goACLs []ACL, flags int) StringResult {
	cpath := C.CString(path)
	defer C.free(unsafe.Pointer(cpath))

	cvalue := C.CString(value)
	defer C.free(unsafe.Pointer(cvalue))

	// Convert ACL from go struct to C struct.
	cACLs := make([]C.struct_ACL, len(goACLs))
	tmps := make([][]byte, 0, 2 * len(goACLs))

	for i := 0; i < len(goACLs); i++ {
		schemeBytes := []byte(goACLs[i].Scheme)
		idBytes := []byte(goACLs[i].Id)
		tmps = append(tmps, schemeBytes, idBytes)
		cACLs[i].perms = C.int32_t(goACLs[i].Perms)
		cACLs[i].id.scheme = (*C.char)(unsafe.Pointer(&schemeBytes[0]))
		cACLs[i].id.id = (*C.char)(unsafe.Pointer(&idBytes[0]))
	}

	var cVector C.struct_ACL_vector

	cVector.count = C.int32_t(len(goACLs))
	cVector.data = &cACLs[0]

	res := make(chan StringResult, 1)

	rc,err := C.zoo_acreate(
		zh.handle,
		cpath,
		cvalue,
		C.int(len(value)),
		&cVector,
		C.int(flags),
		C.string_completion_t(C.my_string_completion),
		unsafe.Pointer(&res))

	var ret StringResult
	if err != nil {
		ret.rc = rc
	} else {
		ret = <-res
	}

	return ret
}

/**
 * \brief delete a node in zookeeper.
 * 
 * \param path the name of the node. Expressed as a file name with slashes 
 * separating ancestors of the node.
 * \param version the expected version of the node. The function will fail if the
 *    actual version of the node does not match the expected version.
 *  If -1 is used the version check will not take place. 
 * \param completion the routine to invoke when the request completes. The completion
 * will be triggered with one of the following codes passed in as the rc argument:
 * ZOK operation completed successfully
 * ZNONODE the node does not exist.
 * ZNOAUTH the client does not have permission.
 * ZBADVERSION expected version does not match actual version.
 * ZNOTEMPTY children are present; node cannot be deleted.
 * \return ZOK on success or one of the following errcodes on failure:
 * ZBADARGUMENTS - invalid input parameters
 * ZINVALIDSTATE - zhandle state is either ZOO_SESSION_EXPIRED_STATE or ZOO_AUTH_FAILED_STATE
 * ZMARSHALLINGERROR - failed to marshall a request; possibly, out of memory
 */
func (zh *ZHandle) Delete(path string, version int) int {
	cpath := C.CString(path)
	defer C.free(unsafe.Pointer(cpath))

	res := make(chan int, 1)
	rc,err := C.zoo_adelete(
		zh.handle,
		cpath,
		C.int(version),
		C.void_completion_t(C.my_void_completion),
		unsafe.Pointer(&res))

	var ret int
	if err != nil {
		ret = int(rc)
	} else {
		ret = <-res
	}

	return ret
}
