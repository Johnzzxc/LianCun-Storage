/*
 * Minio Cloud Storage, (C) 2019 Minio, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cmd

import (
	"bufio"
	"context"
	"errors"
	"net/http"
	"sort"
	"strconv"
	"time"

	"github.com/gorilla/mux"
	"github.com/minio/minio/pkg/dsync"
)

const (
	// Lock maintenance interval.
	lockMaintenanceInterval = 1 * time.Minute

	// Lock validity duration
	lockValidityDuration = 20 * time.Second
)

// To abstract a node over network.
type lockRESTServer struct {
	ll *localLocker
}

func (l *lockRESTServer) writeErrorResponse(w http.ResponseWriter, err error) {
	w.WriteHeader(http.StatusForbidden)
	w.Write([]byte(err.Error()))
}

// IsValid - To authenticate and verify the time difference.
func (l *lockRESTServer) IsValid(w http.ResponseWriter, r *http.Request) bool {
	if l.ll == nil {
		l.writeErrorResponse(w, errLockNotInitialized)
		return false
	}

	if err := storageServerRequestValidate(r); err != nil {
		l.writeErrorResponse(w, err)
		return false
	}
	return true
}

func getLockArgs(r *http.Request) (args dsync.LockArgs, err error) {
	quorum, err := strconv.Atoi(r.URL.Query().Get(lockRESTQuorum))
	if err != nil {
		return args, err
	}

	args = dsync.LockArgs{
		Owner:  r.URL.Query().Get(lockRESTOwner),
		UID:    r.URL.Query().Get(lockRESTUID),
		Source: r.URL.Query().Get(lockRESTSource),
		Quorum: quorum,
	}

	var resources []string
	bio := bufio.NewScanner(r.Body)
	for bio.Scan() {
		resources = append(resources, bio.Text())
	}

	if err := bio.Err(); err != nil {
		return args, err
	}

	sort.Strings(resources)
	args.Resources = resources
	return args, nil
}

// HealthHandler returns success if request is authenticated.
func (l *lockRESTServer) HealthHandler(w http.ResponseWriter, r *http.Request) {
	l.IsValid(w, r)
}

// RefreshHandler - refresh the current lock
func (l *lockRESTServer) RefreshHandler(w http.ResponseWriter, r *http.Request) {
	if !l.IsValid(w, r) {
		l.writeErrorResponse(w, errors.New("invalid request"))
		return
	}

	args, err := getLockArgs(r)
	if err != nil {
		l.writeErrorResponse(w, err)
		return
	}

	refreshed, err := l.ll.Refresh(r.Context(), args)
	if err != nil {
		l.writeErrorResponse(w, err)
		return
	}

	if !refreshed {
		l.writeErrorResponse(w, errLockNotFound)
		return
	}
}

// LockHandler - Acquires a lock.
func (l *lockRESTServer) LockHandler(w http.ResponseWriter, r *http.Request) {
	if !l.IsValid(w, r) {
		l.writeErrorResponse(w, errors.New("invalid request"))
		return
	}

	args, err := getLockArgs(r)
	if err != nil {
		l.writeErrorResponse(w, err)
		return
	}

	success, err := l.ll.Lock(r.Context(), args)
	if err == nil && !success {
		err = errLockConflict
	}
	if err != nil {
		l.writeErrorResponse(w, err)
		return
	}
}

// UnlockHandler - releases the acquired lock.
func (l *lockRESTServer) UnlockHandler(w http.ResponseWriter, r *http.Request) {
	if !l.IsValid(w, r) {
		l.writeErrorResponse(w, errors.New("invalid request"))
		return
	}

	args, err := getLockArgs(r)
	if err != nil {
		l.writeErrorResponse(w, err)
		return
	}

	_, err = l.ll.Unlock(args)
	// Ignore the Unlock() "reply" return value because if err == nil, "reply" is always true
	// Consequently, if err != nil, reply is always false
	if err != nil {
		l.writeErrorResponse(w, err)
		return
	}
}

// LockHandler - Acquires an RLock.
func (l *lockRESTServer) RLockHandler(w http.ResponseWriter, r *http.Request) {
	if !l.IsValid(w, r) {
		l.writeErrorResponse(w, errors.New("invalid request"))
		return
	}

	args, err := getLockArgs(r)
	if err != nil {
		l.writeErrorResponse(w, err)
		return
	}

	success, err := l.ll.RLock(r.Context(), args)
	if err == nil && !success {
		err = errLockConflict
	}
	if err != nil {
		l.writeErrorResponse(w, err)
		return
	}
}

// RUnlockHandler - releases the acquired read lock.
func (l *lockRESTServer) RUnlockHandler(w http.ResponseWriter, r *http.Request) {
	if !l.IsValid(w, r) {
		l.writeErrorResponse(w, errors.New("invalid request"))
		return
	}

	args, err := getLockArgs(r)
	if err != nil {
		l.writeErrorResponse(w, err)
		return
	}

	// Ignore the RUnlock() "reply" return value because if err == nil, "reply" is always true.
	// Consequently, if err != nil, reply is always false
	if _, err = l.ll.RUnlock(args); err != nil {
		l.writeErrorResponse(w, err)
		return
	}
}

// ForceUnlockHandler - query expired lock status.
func (l *lockRESTServer) ForceUnlockHandler(w http.ResponseWriter, r *http.Request) {
	if !l.IsValid(w, r) {
		l.writeErrorResponse(w, errors.New("invalid request"))
		return
	}

	args, err := getLockArgs(r)
	if err != nil {
		l.writeErrorResponse(w, err)
		return
	}

	if _, err = l.ll.ForceUnlock(r.Context(), args); err != nil {
		l.writeErrorResponse(w, err)
		return
	}
}

// lockMaintenance loops over all locks and discards locks
// that have not been refreshed for some time.
func lockMaintenance(ctx context.Context) {
	// Wait until the object API is ready
	// no need to start the lock maintenance
	// if ObjectAPI is not initialized.

	var objAPI ObjectLayer

	for {
		objAPI = newObjectLayerFn()
		if objAPI == nil {
			time.Sleep(time.Second)
			continue
		}
		break
	}

	if _, ok := objAPI.(*erasureServerPools); !ok {
		return
	}

	// Initialize a new ticker with 1 minute between each ticks.
	lkTimer := time.NewTimer(lockMaintenanceInterval)
	// Stop the timer upon returning.
	defer lkTimer.Stop()

	for {
		// Verifies every minute for locks held more than 2 minutes.
		select {
		case <-ctx.Done():
			return
		case <-lkTimer.C:
			// Reset the timer for next cycle.
			lkTimer.Reset(lockMaintenanceInterval)

			globalLockServer.expireOldLocks(lockValidityDuration)
		}
	}
}

// registerLockRESTHandlers - register lock rest router.
func registerLockRESTHandlers(router *mux.Router) {
	lockServer := &lockRESTServer{
		ll: newLocker(),
	}

	subrouter := router.PathPrefix(lockRESTPrefix).Subrouter()
	subrouter.Methods(http.MethodPost).Path(lockRESTVersionPrefix + lockRESTMethodHealth).HandlerFunc(httpTraceHdrs(lockServer.HealthHandler))
	subrouter.Methods(http.MethodPost).Path(lockRESTVersionPrefix + lockRESTMethodRefresh).HandlerFunc(httpTraceHdrs(lockServer.RefreshHandler))
	subrouter.Methods(http.MethodPost).Path(lockRESTVersionPrefix + lockRESTMethodLock).HandlerFunc(httpTraceHdrs(lockServer.LockHandler))
	subrouter.Methods(http.MethodPost).Path(lockRESTVersionPrefix + lockRESTMethodRLock).HandlerFunc(httpTraceHdrs(lockServer.RLockHandler))
	subrouter.Methods(http.MethodPost).Path(lockRESTVersionPrefix + lockRESTMethodUnlock).HandlerFunc(httpTraceHdrs(lockServer.UnlockHandler))
	subrouter.Methods(http.MethodPost).Path(lockRESTVersionPrefix + lockRESTMethodRUnlock).HandlerFunc(httpTraceHdrs(lockServer.RUnlockHandler))
	subrouter.Methods(http.MethodPost).Path(lockRESTVersionPrefix + lockRESTMethodForceUnlock).HandlerFunc(httpTraceAll(lockServer.ForceUnlockHandler))

	globalLockServer = lockServer.ll

	go lockMaintenance(GlobalContext)
}
