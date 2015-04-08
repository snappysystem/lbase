package zk

import (
	"C"
	"unsafe"
)

//export GoStatCompletion
func GoStatCompletion(rc C.int, vstat unsafe.Pointer, data unsafe.Pointer) {
	GoStatCompletion2(rc, vstat, data)
}

//export GoDataCompletion
func GoDataCompletion(
	rc C.int,
	value unsafe.Pointer,
	value_len C.int,
	stat, data unsafe.Pointer) {

	GoDataCompletion2(rc, value, value_len, stat, data)
}

//export GoWatcher
func GoWatcher(Type C.int, state C.int, path unsafe.Pointer, ctx unsafe.Pointer) {
	GoWatcher2(Type, state, path, ctx)
}
