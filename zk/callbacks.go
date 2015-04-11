package zk

/*
#cgo CFLAGS: -I/usr/include
#include <zookeeper/zookeeper.h>
*/
import "C"

import (
	"unsafe"
	"reflect"
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

//export GoStringsCompletion
func GoStringsCompletion(rc C.int, strings, data unsafe.Pointer) {
	ch := (*chan StringsResult)(data)
	strVec := (*C.struct_String_vector)(strings)
	strs := make([]string, 0, strVec.count)

	// Simulate a go slice.
	ppvHdr := reflect.SliceHeader{
		Data: uintptr(unsafe.Pointer(strVec.data)),
		Len: int(strVec.count),
		Cap: int(strVec.count),
	}
	goSlice := *(*[]unsafe.Pointer)(unsafe.Pointer(&ppvHdr))

	for _,s := range goSlice {
		strs = append(strs, C.GoString((*C.char)(s)))
	}

	result := StringsResult{
		rc: rc,
		strings: strs,
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
