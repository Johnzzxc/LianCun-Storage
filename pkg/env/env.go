/*
 * MinIO Cloud Storage, (C) 2019-2020 MinIO, Inc.
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
 *
 */

package env

import (
	"strconv"
	"strings"
	"sync"
)

var (
	privateMutex sync.RWMutex
	lockEnvMutex sync.Mutex
	envOff       bool
)

// LockSetEnv locks modifications to environment.
// Call returned function to unlock.
func LockSetEnv() func() {
	lockEnvMutex.Lock()
	return lockEnvMutex.Unlock
}

// SetEnvOff - turns off env lookup
// A global lock above this MUST ensure that
func SetEnvOff() {
	privateMutex.Lock()
	defer privateMutex.Unlock()

	envOff = true
}

// SetEnvOn - turns on env lookup
func SetEnvOn() {
	privateMutex.Lock()
	defer privateMutex.Unlock()

	envOff = false
}

// IsSet returns if the given env key is set.
func IsSet(key string) bool {
	_, _, _, ok := LookupEnv(key)
	return ok
}

// Get retrieves the value of the environment variable named
// by the key. If the variable is present in the environment the
// value (which may be empty) is returned. Otherwise it returns
// the specified default value.
func Get(key, defaultValue string) string {
	privateMutex.RLock()
	ok := envOff
	privateMutex.RUnlock()
	if ok {
		return defaultValue
	}
	if v, _, _, ok := LookupEnv(key); ok {
		return v
	}
	return defaultValue
}

// GetInt returns an integer if found in the environment
// and returns the default value otherwise.
func GetInt(key string, defaultValue int) (int, error) {
	v := Get(key, "")
	if v == "" {
		return defaultValue, nil
	}
	return strconv.Atoi(v)
}

// List all envs with a given prefix.
func List(prefix string) (envs []string) {
	for _, env := range Environ() {
		if strings.HasPrefix(env, prefix) {
			values := strings.SplitN(env, "=", 2)
			if len(values) == 2 {
				envs = append(envs, values[0])
			}
		}
	}
	return envs
}
