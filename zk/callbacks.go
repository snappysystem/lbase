package zk

/*
#cgo CFLAGS: -I/usr/include
#include <zookeeper/zookeeper.h>
*/
import "C"

import (
	"unsafe"
)

//export GoStatCompletion
func GoStatCompletion(rc C.int, vstat unsafe.Pointer, data unsafe.Pointer) {
	stat := (*C.struct_Stat)(vstat)
	ch := (*chan StatResult)(data)
	result := StatResult{
		rc : rc,
		stat : *stat,
	}
	(*ch) <-result
}

//export GoDataCompletion
func GoDataCompletion(
	rc C.int,
	value unsafe.Pointer,
	value_len C.int,
	stat, data unsafe.Pointer) {

	ch := (*chan DataResult)(data)
	result := DataResult{
		rc: rc,
		data: C.GoBytes(value, value_len),
		stat: *(*C.struct_Stat)(stat),
	}

	(*ch) <-result
}

//export GoWatcher
func GoWatcher(Type C.int, state C.int, path unsafe.Pointer, ctx unsafe.Pointer) {
	watcher := Watcher{
		Type: int(Type),
		State: int(state),
		Path: C.GoString((*C.char)(path)),
	}
	ch := (*chan Watcher)(ctx)
	(*ch) <- watcher
}
