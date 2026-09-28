//go:build !windows
// +build !windows

/*
Copyright The Velero Contributors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package kopia

import (
	"bytes"
	"context"
	"errors"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/kopia/kopia/fs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeBlockFile is a snapshot file backed by an in-memory buffer. Its readers return
// short reads and can fail at a given offset, like a remote object reader would.
type fakeBlockFile struct {
	fs.Entry

	data   []byte
	failAt int64
	opened atomic.Int32
}

func (f *fakeBlockFile) Name() string { return "block" }
func (f *fakeBlockFile) Size() int64  { return int64(len(f.data)) }

func (f *fakeBlockFile) Open(ctx context.Context) (fs.Reader, error) {
	f.opened.Add(1)
	return &fakeBlockReader{Reader: bytes.NewReader(f.data), failAt: f.failAt}, nil
}

type fakeBlockReader struct {
	*bytes.Reader
	failAt int64
}

func (r *fakeBlockReader) Read(p []byte) (int, error) {
	offset := r.Size() - int64(r.Len())
	if r.failAt > 0 && offset >= r.failAt {
		return 0, errors.New("remote read failed")
	}
	return r.Reader.Read(p[:min(len(p), 1000)])
}

func (r *fakeBlockReader) Close() error             { return nil }
func (r *fakeBlockReader) Entry() (fs.Entry, error) { return nil, nil }

func restoreToTempFile(t *testing.T, remote *fakeBlockFile, parallelism int) ([]byte, int64, error) {
	t.Helper()

	target := filepath.Join(t.TempDir(), "target")
	o := &BlockOutput{Parallelism: parallelism, targetFileName: target}

	var restored atomic.Int64
	err := o.WriteFile(context.Background(), "", remote, func(n int64) { restored.Add(n) })
	require.NoError(t, o.Terminate())

	data, readErr := os.ReadFile(target)
	require.NoError(t, readErr)
	return data, restored.Load(), err
}

func TestBlockOutputWriteFile(t *testing.T) {
	defer func(v int64) { minBlockRestoreSegment = v }(minBlockRestoreSegment)
	minBlockRestoreSegment = 1 << 20

	data := make([]byte, 10<<20+12345)
	_, err := rand.New(rand.NewSource(1)).Read(data)
	require.NoError(t, err)

	tests := []struct {
		name        string
		parallelism int
		wantOpened  int32
	}{
		{name: "sequential", parallelism: 1, wantOpened: 1},
		{name: "parallel", parallelism: 4, wantOpened: 4},
		// 10 workers by size, but 1 MiB alignment rounds each range up to 2 MiB.
		{name: "capped by size", parallelism: 64, wantOpened: 6},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			remote := &fakeBlockFile{data: data}

			restored, progress, err := restoreToTempFile(t, remote, tc.parallelism)

			require.NoError(t, err)
			assert.True(t, bytes.Equal(data, restored), "restored data differs from the source")
			assert.Equal(t, int64(len(data)), progress)
			assert.Equal(t, tc.wantOpened, remote.opened.Load())
		})
	}
}

func TestBlockOutputWriteFileReadError(t *testing.T) {
	defer func(v int64) { minBlockRestoreSegment = v }(minBlockRestoreSegment)
	minBlockRestoreSegment = 1 << 20

	remote := &fakeBlockFile{data: make([]byte, 8<<20), failAt: 5 << 20}

	for _, parallelism := range []int{1, 4} {
		_, _, err := restoreToTempFile(t, remote, parallelism)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "remote read failed")
	}
}

func TestBlockRestoreWorkers(t *testing.T) {
	tests := []struct {
		name        string
		parallelism int
		size        int64
		want        int
	}{
		{name: "sequential", parallelism: 1, size: 100 << 30, want: 1},
		{name: "unset", parallelism: 0, size: 100 << 30, want: 0},
		{name: "small volume", parallelism: 8, size: 100 << 20, want: 1},
		{name: "medium volume", parallelism: 8, size: 256 << 20, want: 4},
		{name: "large volume", parallelism: 8, size: 20 << 30, want: 8},
		{name: "capped", parallelism: 64, size: 400 << 30, want: maxBlockRestoreWorkers},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, blockRestoreWorkers(tc.parallelism, tc.size))
		})
	}
}

var _ io.ReadSeeker = &fakeBlockReader{}
