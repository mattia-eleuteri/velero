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
	"context"
	"io"
	"os"
	"path/filepath"
	"sync"
	"syscall"

	"github.com/cockroachdb/errors"
	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/snapshot/restore"
	"golang.org/x/sync/errgroup"
)

type BlockOutput struct {
	*restore.FilesystemOutput

	// Parallelism is the number of concurrent readers used to restore the volume.
	// Values lower than 2 restore the volume sequentially.
	Parallelism int

	targetFileName string
	targetFile     *os.File
}

var _ restore.Output = &BlockOutput{}

const (
	bufferSize = 128 * 1024

	// maxBlockRestoreWorkers caps the number of concurrent readers for a single volume.
	maxBlockRestoreWorkers = 16
	// blockRestoreSegmentAlign keeps range boundaries aligned for the block device.
	blockRestoreSegmentAlign = 1 << 20
)

// minBlockRestoreSegment is the smallest range assigned to one reader, so small
// volumes are not split into ranges that cost more to open than to read.
var minBlockRestoreSegment int64 = 64 << 20

func (o *BlockOutput) WriteFile(ctx context.Context, relativePath string, remoteFile fs.File, progressCb restore.FileWriteProgress) error {
	targetFile, err := os.Create(o.targetFileName)
	if err != nil {
		return errors.Wrapf(err, "failed to open file %s", o.targetFileName)
	}
	o.targetFile = targetFile

	size := remoteFile.Size()
	workers := blockRestoreWorkers(o.Parallelism, size)
	if workers < 2 {
		return o.writeSequential(ctx, remoteFile, targetFile, progressCb)
	}

	// Kopia reads the chunks of a single object one after the other, so a large volume
	// restored through one reader is bound by the latency of each chunk fetch. Each
	// worker opens its own reader, seeks to its range and writes at the matching offset.
	segment := (size + int64(workers) - 1) / int64(workers)
	segment = (segment + blockRestoreSegmentAlign - 1) / blockRestoreSegmentAlign * blockRestoreSegmentAlign

	var progressMu sync.Mutex
	progress := func(n int64) {
		progressMu.Lock()
		defer progressMu.Unlock()
		progressCb(n)
	}

	g, gctx := errgroup.WithContext(ctx)
	for start := int64(0); start < size; start += segment {
		start, end := start, min(start+segment, size)
		g.Go(func() error {
			return copyBlockRange(gctx, remoteFile, targetFile, start, end, progress)
		})
	}

	return g.Wait()
}

func blockRestoreWorkers(parallelism int, size int64) int {
	workers := min(parallelism, maxBlockRestoreWorkers)
	if bySize := size / minBlockRestoreSegment; bySize < int64(workers) {
		workers = int(bySize)
	}
	return workers
}

func copyBlockRange(ctx context.Context, remoteFile fs.File, targetFile *os.File, start, end int64, progress func(int64)) error {
	remoteReader, err := remoteFile.Open(ctx)
	if err != nil {
		return errors.Wrapf(err, "failed to open remote file %s", remoteFile.Name())
	}
	defer remoteReader.Close()

	if _, err := remoteReader.Seek(start, io.SeekStart); err != nil {
		return errors.Wrapf(err, "failed to seek remote file %s to offset %d", remoteFile.Name(), start)
	}

	buffer := make([]byte, bufferSize)
	for offset := start; offset < end; {
		if err := ctx.Err(); err != nil {
			return err
		}

		n, err := io.ReadFull(remoteReader, buffer[:min(int64(len(buffer)), end-offset)])
		if n > 0 {
			if _, werr := targetFile.WriteAt(buffer[:n], offset); werr != nil {
				return errors.Wrapf(werr, "failed to write data to file %s at offset %d", targetFile.Name(), offset)
			}
			offset += int64(n)
			progress(int64(n))
		}

		if err != nil {
			if (err == io.EOF || err == io.ErrUnexpectedEOF) && offset == end {
				break
			}
			return errors.Wrapf(err, "failed to read data from remote file %s at offset %d", remoteFile.Name(), offset)
		}
	}

	return nil
}

func (o *BlockOutput) writeSequential(ctx context.Context, remoteFile fs.File, targetFile *os.File, progressCb restore.FileWriteProgress) error {
	remoteReader, err := remoteFile.Open(ctx)
	if err != nil {
		return errors.Wrapf(err, "failed to open remote file %s", remoteFile.Name())
	}
	defer remoteReader.Close()

	buffer := make([]byte, bufferSize)

	readData := true
	for readData {
		bytesToWrite, err := remoteReader.Read(buffer)
		if err != nil {
			if err != io.EOF {
				return errors.Wrapf(err, "failed to read data from remote file %s", o.targetFileName)
			}
			readData = false
		}

		if bytesToWrite > 0 {
			offset := 0
			for bytesToWrite > 0 {
				if bytesWritten, err := targetFile.Write(buffer[offset:bytesToWrite]); err == nil {
					progressCb(int64(bytesWritten))
					bytesToWrite -= bytesWritten
					offset += bytesWritten
				} else {
					return errors.Wrapf(err, "failed to write data to file %s", o.targetFileName)
				}
			}
		}
	}

	return nil
}

func (o *BlockOutput) BeginDirectory(ctx context.Context, relativePath string, e fs.Directory) error {
	var err error
	o.targetFileName, err = filepath.EvalSymlinks(o.TargetPath)
	if err != nil {
		return errors.Wrapf(err, "unable to evaluate symlinks for %s", o.targetFileName)
	}

	fileInfo, err := os.Lstat(o.targetFileName)
	if err != nil {
		return errors.Wrapf(err, "unable to get the target device information for %s", o.TargetPath)
	}

	if (fileInfo.Sys().(*syscall.Stat_t).Mode & syscall.S_IFMT) != syscall.S_IFBLK {
		return errors.Errorf("target file %s is not a block device", o.TargetPath)
	}

	return nil
}

func (o *BlockOutput) Flush() error {
	if o.targetFile != nil {
		if err := o.targetFile.Sync(); err != nil {
			return errors.Wrapf(err, "error syncing block dev %v", o.targetFileName)
		}
	}

	return nil
}

func (o *BlockOutput) Terminate() error {
	if o.targetFile != nil {
		if err := o.targetFile.Close(); err != nil {
			return errors.Wrapf(err, "error closing block dev %v", o.targetFileName)
		}
	}

	return nil
}
