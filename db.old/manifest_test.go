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
package db

import (
	"os"
	"testing"
)

func TestManifestName(t *testing.T) {
	numbers := []int64{0, 1, 5, 1024, 10097}

	for _, num := range numbers {
		base := MakeManifestName(num)
		res := ParseManifestName(base)

		if res != num {
			t.Error("Fails to test name")
		}
	}
}

func TestRecoverManifestSuccess(t *testing.T) {
	parent := "/tmp/manifest_test/RecoverManifestSuccess"

	os.RemoveAll(parent)
	os.MkdirAll(parent, os.ModePerm)

	env := MakeNativeEnv()
	manifest := RecoverManifest(env, parent, true)

	if manifest == nil {
		t.Error("Fails to create a new manifest")
	}
}

func TestRecoverManifestFail(t *testing.T) {
	parent := "/tmp/manifest_test/RecoverManifestSuccess"

	os.RemoveAll(parent)
	os.MkdirAll(parent, os.ModePerm)

	env := MakeNativeEnv()
	manifest := RecoverManifest(env, parent, false)

	if manifest != nil {
		t.Error("Fails to create a new manifest")
	}
}

func TestSingleManifestSession(t *testing.T) {
	parent := "/tmp/manifest_test/SingleManifestSession"

	os.RemoveAll(parent)
	os.MkdirAll(parent, os.ModePerm)

	env := MakeNativeEnv()
	manifest := RecoverManifest(env, parent, true)

	fileId := manifest.CreateFile(false)
	req := NewSnapshotRequest{
		Levels: [][]int64{[]int64{fileId}},
		Files:  make(map[int64]FileInfo),
	}

	req.Files[fileId] = FileInfo{Location: "foo"}

	sId := manifest.NewSnapshot(&req, false)
	manifest.Close()

	// try to load manifest again
	env = MakeNativeEnv()
	manifest2 := RecoverManifest(env, parent, false)
	if manifest2 == nil {
		t.Error("Fails to recover")
	}

	infos := manifest2.GetSnapshotInfo(sId)
	if len(infos) != 1 || len(infos[0]) != 1 || infos[0][0].Location != "foo" {
		t.Error("Fails to restore file info", len(infos))
	}
}
