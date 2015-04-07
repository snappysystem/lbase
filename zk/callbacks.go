package zk

import (
	"C"
	"unsafe"
)

//export GoStatCompletion
func GoStatCompletion(rc C.int, vstat unsafe.Pointer, data unsafe.Pointer) {
	GoStatCompletion2(rc, vstat, data)
}
