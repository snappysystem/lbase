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
