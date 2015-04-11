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

void my_void_completion(int rc, const void *data) {
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

void my_acl_completion(int rc, struct ACL_vector *acl,
       struct Stat *stat, const void *data) {
}
*/
import "C"

import (
	"unsafe"
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

/** Zookeeper ACL constants */
var (
	/** This is a completely open ACL*/
	ZOO_OPEN_ACL *C.struct_ACL_vector = &C.ZOO_OPEN_ACL_UNSAFE
	/** This ACL gives the world the ability to read. */
	ZOO_READ_ACL *C.struct_ACL_vector = &C.ZOO_READ_ACL_UNSAFE
	/** This ACL gives the creators authentication id's all permissions. */
	ZOO_CREATOR_ALL_ACL *C.struct_ACL_vector = &C.ZOO_CREATOR_ALL_ACL
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
	ZOO_EXPIRED_SESSION_STATE = C.ZOO_EXPIRED_SESSION_STATE
	ZOO_AUTH_FAILED_STATE = C.ZOO_AUTH_FAILED_STATE
	ZOO_CONNECTING_STATE = C.ZOO_CONNECTING_STATE
	ZOO_ASSOCIATING_STATE = C.ZOO_ASSOCIATING_STATE
	ZOO_CONNECTED_STATE = C.ZOO_CONNECTED_STATE
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
	ZOO_CREATED_EVENT = C.ZOO_CREATED_EVENT
	/**
	 * \brief a node has been deleted.
	 * 
	 * This is only generated by watches on nodes. These watches
	 * are set using \ref zoo_exists and \ref zoo_get.
	 */
	ZOO_DELETED_EVENT = C.ZOO_DELETED_EVENT
	/**
	 * \brief a node has changed.
	 * 
	 * This is only generated by watches on nodes. These watches
	 * are set using \ref zoo_exists and \ref zoo_get.
	 */
	ZOO_CHANGED_EVENT = C.ZOO_CHANGED_EVENT
	/**
	 * \brief a change as occurred in the list of children.
	 * 
	 * This is only generated by watches on the child list of a node. These watches
	 * are set using \ref zoo_get_children or \ref zoo_get_children2.
	 */
	ZOO_CHILD_EVENT = C.ZOO_CHILD_EVENT
	/**
	 * \brief a session has been lost.
	 * 
	 * This is generated when a client loses contact or reconnects with a server.
	 */
	ZOO_SESSION_EVENT = C.ZOO_SESSION_EVENT
	/**
	 * \brief a watch has been removed.
	 * 
	 * This is generated when the server for some reason, probably a resource
	 * constraint, will no longer watch a node for a client.
	 */
	ZOO_NOTWATCHING_EVENT = C.ZOO_NOTWATCHING_EVENT
)


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


type ZkID struct {
	id C.clientid_t
}

type ZHandle struct {
	handle *C.zhandle_t
}

/**
 * \brief create a new Zookeeper handle.
 * 
 * The return value is the zookeeper handle.
 */
func NewZHandle(hosts string, recvTimeout int, id *ZkID) (h ZHandle, ok bool) {
	chosts := C.CString(hosts)
	defer C.free(unsafe.Pointer(chosts))

	handle, err := C.zookeeper_init(
		chosts, (C.watcher_fn)(C.my_global_watcher), C.int(recvTimeout), &id.id, nil, 0)

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
