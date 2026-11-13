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

package csi

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	snapshotv1api "github.com/kubernetes-csi/external-snapshotter/client/v8/apis/volumesnapshot/v1"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"

	velerotest "github.com/vmware-tanzu/velero/pkg/test"
)

// getHookClient lets a test fail chosen Gets.
type getHookClient struct {
	crclient.Client
	get func(ctx context.Context, key crclient.ObjectKey, obj crclient.Object, opts ...crclient.GetOption) error
}

func (c *getHookClient) Get(ctx context.Context, key crclient.ObjectKey, obj crclient.Object, opts ...crclient.GetOption) error {
	return c.get(ctx, key, obj, opts...)
}

// limiterErr is what client-go returns when its rate limiter would have to
// wait past the context deadline.
var limiterErr = fmt.Errorf("client rate limiter Wait returned an error: %w",
	errors.New("rate: Wait(n=1) would exceed context deadline"))

func shortenEarlyPoll(t *testing.T) {
	t.Helper()
	t.Setenv("CSI_SNAPSHOT_EARLY_FREQUENT_POLLING", "true")
	interval, timeout := earlyPollInterval, earlyPollTimeout
	earlyPollInterval, earlyPollTimeout = 20*time.Millisecond, 100*time.Millisecond
	t.Cleanup(func() { earlyPollInterval, earlyPollTimeout = interval, timeout })
}

func TestIsPollDeadlineError(t *testing.T) {
	expired, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()

	assert.True(t, isPollDeadlineError(context.Background(), limiterErr))
	assert.True(t, isPollDeadlineError(context.Background(), context.DeadlineExceeded))
	assert.True(t, isPollDeadlineError(expired, errors.New("anything")))
	assert.False(t, isPollDeadlineError(context.Background(), errors.New("volumesnapshots.snapshot.storage.k8s.io \"vs\" not found")))
}

// A rate limiter error during the early frequent polling must fall back to
// the slow polling instead of failing the snapshot (velero-io/velero#10727).
func TestWaitUntilVSCHandleIsReadyEarlyPollLimiterFallsBack(t *testing.T) {
	shortenEarlyPoll(t)

	vscName := "vsc"
	handle := "snapshot-handle"
	vs := &snapshotv1api.VolumeSnapshot{
		ObjectMeta: metav1.ObjectMeta{Name: "vs", Namespace: "default"},
		Status:     &snapshotv1api.VolumeSnapshotStatus{BoundVolumeSnapshotContentName: &vscName},
	}
	vsc := &snapshotv1api.VolumeSnapshotContent{
		ObjectMeta: metav1.ObjectMeta{Name: vscName},
		Status:     &snapshotv1api.VolumeSnapshotContentStatus{SnapshotHandle: &handle},
	}

	// Every Get fails with the limiter error until the early phase is over.
	start := time.Now()
	c := velerotest.NewFakeControllerRuntimeClient(t, []runtime.Object{vs, vsc}...)
	client := &getHookClient{Client: c, get: func(ctx context.Context, key crclient.ObjectKey, obj crclient.Object, opts ...crclient.GetOption) error {
		if time.Since(start) < earlyPollTimeout {
			return limiterErr
		}
		return c.Get(ctx, key, obj, opts...)
	}}

	got, err := WaitUntilVSCHandleIsReady(vs, client, logrus.New().WithField("test", t.Name()), time.Second)
	require.NoError(t, err)
	assert.Equal(t, handle, *got.Status.SnapshotHandle)
}

// When the snapshot never gets a handle, the final timeout reports the
// VolumeSnapshotContent error, not the rate limiter error.
func TestWaitUntilVSCHandleIsReadyTimeoutReportsVSCError(t *testing.T) {
	shortenEarlyPoll(t)

	vscName := "vsc"
	message := "Failed to suspend IO for resource pvc-x on layer DrbdLayer"
	vs := &snapshotv1api.VolumeSnapshot{
		ObjectMeta: metav1.ObjectMeta{Name: "vs", Namespace: "default"},
		Status:     &snapshotv1api.VolumeSnapshotStatus{BoundVolumeSnapshotContentName: &vscName},
	}
	vsc := &snapshotv1api.VolumeSnapshotContent{
		ObjectMeta: metav1.ObjectMeta{Name: vscName},
		Status: &snapshotv1api.VolumeSnapshotContentStatus{
			Error: &snapshotv1api.VolumeSnapshotError{Message: &message},
		},
	}

	// The first VolumeSnapshotContent Get succeeds, every later one hits the
	// limiter.
	var vscGets atomic.Int32
	c := velerotest.NewFakeControllerRuntimeClient(t, []runtime.Object{vs, vsc}...)
	client := &getHookClient{Client: c, get: func(ctx context.Context, key crclient.ObjectKey, obj crclient.Object, opts ...crclient.GetOption) error {
		if _, ok := obj.(*snapshotv1api.VolumeSnapshotContent); ok && vscGets.Add(1) > 1 {
			return limiterErr
		}
		return c.Get(ctx, key, obj, opts...)
	}}

	_, err := WaitUntilVSCHandleIsReady(vs, client, logrus.New().WithField("test", t.Name()), time.Second)
	require.Error(t, err)
	assert.Contains(t, err.Error(), message)
	assert.NotContains(t, err.Error(), "rate limiter")
}
