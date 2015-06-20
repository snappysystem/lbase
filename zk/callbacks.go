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
#include <zookeeper/zookeeper.h>
*/
import "C"

import (
	"unsafe"
	"reflect"
)

//export GoVoidCompletion
func GoVoidCompletion(rc C.int, data unsafe.Pointer) {
	ch := (*chan int)(data)
	(*ch) <-int(rc)
}

//export GoStatCompletion
func GoStatCompletion(rc C.int, vstat unsafe.Pointer, data unsafe.Pointer) {
	stat := (*C.struct_Stat)(vstat)
	ch := (*chan StatResult)(data)

	var result StatResult
	if stat != nil {
		result = StatResult{
			rc : rc,
			stat : *stat,
		}
	} else {
		result = StatResult{
			rc : rc,
		}
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
	var result DataResult
	if value != nil && stat != nil {
		result = DataResult{
			rc: rc,
			data: C.GoBytes(value, value_len),
			stat: *(*C.struct_Stat)(stat),
		}
	} else {
		result.rc = rc
		if value != nil {
			result.data = C.GoBytes(value, value_len)
		}
		if stat != nil {
			result.stat = *(*C.struct_Stat)(stat)
		}
	}

	(*ch) <-result
}

//export GoStringsCompletion
func GoStringsCompletion(rc C.int, strings, data unsafe.Pointer) {
	ch := (*chan StringsResult)(data)
	strVec := (*C.struct_String_vector)(strings)
	strs := make([]string, 0, strVec.count)

	if strings == nil {
		(*ch) <-StringsResult{rc: rc}
		return
	}

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

//export GoStringsStatCompletion
func GoStringsStatCompletion(rc C.int, strings, stat, data unsafe.Pointer) {
	ch := (*chan StringsStatResult)(data)
	strVec := (*C.struct_String_vector)(strings)
	var strs []string

	if strings != nil {
		// Simulate a go slice.
		strs := make([]string, 0, strVec.count)
		ppvHdr := reflect.SliceHeader{
			Data: uintptr(unsafe.Pointer(strVec.data)),
			Len: int(strVec.count),
			Cap: int(strVec.count),
		}
		goSlice := *(*[]unsafe.Pointer)(unsafe.Pointer(&ppvHdr))

		for _,s := range goSlice {
			strs = append(strs, C.GoString((*C.char)(s)))
		}
	}

	if stat != nil {
		result := StringsStatResult{
			rc: rc,
			strings: strs,
			stat: *(*C.struct_Stat)(stat),
		}

		(*ch) <-result
	} else {
		result := StringsStatResult{
			rc: rc,
			strings: strs,
		}

		(*ch) <-result
	}
}

//export GoStringCompletion
func GoStringCompletion(rc C.int, value, data unsafe.Pointer) {
	ch := (*chan StringResult)(data)
	if value != nil {
		result := StringResult{
			rc: rc,
			str: C.GoString((*C.char)(value)),
		}

		(*ch) <-result
	} else {
		result := StringResult{ rc: rc, }
		(*ch) <-result
	}
}

//export GoACLCompletion
func GoACLCompletion(rc C.int, aclVec, stat, data unsafe.Pointer) {
	ch := (*chan ACLResult)(data)
	var goACLs []ACL

	if aclVec != nil {
		aclCVec := (*C.struct_ACL_vector)(aclVec)
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
	}

	if stat != nil {
		result := ACLResult{
			rc: rc,
			acls: goACLs,
			stat: *(*C.struct_Stat)(stat),
		}

		(*ch) <-result
	} else {
		result := ACLResult{
			rc: rc,
			acls: goACLs,
		}

		(*ch) <-result
	}
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
