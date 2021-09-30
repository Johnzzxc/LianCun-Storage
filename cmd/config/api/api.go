/*
 * MinIO Cloud Storage, (C) 2020 MinIO, Inc.
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

package api

import (
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/minio/minio/cmd/config"
	"github.com/minio/minio/pkg/env"
)

// API sub-system constants
const (
	apiRequestsMax                = "requests_max"
	apiRequestsDeadline           = "requests_deadline"
	apiClusterDeadline            = "cluster_deadline"
	apiCorsAllowOrigin            = "cors_allow_origin"
	apiRemoteTransportDeadline    = "remote_transport_deadline"
	apiListQuorum                 = "list_quorum"
	apiExtendListCacheLife        = "extend_list_cache_life"
	apiReplicationWorkers         = "replication_workers"
	EnvAPIRequestsMax             = "MINIO_API_REQUESTS_MAX"
	EnvAPIRequestsDeadline        = "MINIO_API_REQUESTS_DEADLINE"
	EnvAPIClusterDeadline         = "MINIO_API_CLUSTER_DEADLINE"
	EnvAPICorsAllowOrigin         = "MINIO_API_CORS_ALLOW_ORIGIN"
	EnvAPIRemoteTransportDeadline = "MINIO_API_REMOTE_TRANSPORT_DEADLINE"
	EnvAPIListQuorum              = "MINIO_API_LIST_QUORUM"
	EnvAPIExtendListCacheLife     = "MINIO_API_EXTEND_LIST_CACHE_LIFE"
	EnvAPISecureCiphers           = "MINIO_API_SECURE_CIPHERS"
	EnvAPIReplicationWorkers      = "MINIO_API_REPLICATION_WORKERS"
)

// Deprecated key and ENVs
const (
	apiReadyDeadline    = "ready_deadline"
	EnvAPIReadyDeadline = "MINIO_API_READY_DEADLINE"
)

// DefaultKVS - default storage class config
var (
	DefaultKVS = config.KVS{
		config.KV{
			Key:   apiRequestsMax,
			Value: "0",
		},
		config.KV{
			Key:   apiRequestsDeadline,
			Value: "10s",
		},
		config.KV{
			Key:   apiClusterDeadline,
			Value: "10s",
		},
		config.KV{
			Key:   apiCorsAllowOrigin,
			Value: "*",
		},
		config.KV{
			Key:   apiRemoteTransportDeadline,
			Value: "2h",
		},
		config.KV{
			Key:   apiListQuorum,
			Value: "strict",
		},
		config.KV{
			Key:   apiExtendListCacheLife,
			Value: "0s",
		},
		config.KV{
			Key:   apiReplicationWorkers,
			Value: "100",
		},
	}
)

// Config storage class configuration
type Config struct {
	RequestsMax             int           `json:"requests_max"`
	RequestsDeadline        time.Duration `json:"requests_deadline"`
	ClusterDeadline         time.Duration `json:"cluster_deadline"`
	CorsAllowOrigin         []string      `json:"cors_allow_origin"`
	RemoteTransportDeadline time.Duration `json:"remote_transport_deadline"`
	ListQuorum              string        `json:"list_strict_quorum"`
	ExtendListLife          time.Duration `json:"extend_list_cache_life"`
	ReplicationWorkers      int           `json:"replication_workers"`
}

// UnmarshalJSON - Validate SS and RRS parity when unmarshalling JSON.
func (sCfg *Config) UnmarshalJSON(data []byte) error {
	type Alias Config
	aux := &struct {
		*Alias
	}{
		Alias: (*Alias)(sCfg),
	}
	return json.Unmarshal(data, &aux)
}

// GetListQuorum interprets list quorum values and returns appropriate
// acceptable quorum expected for list operations
func (sCfg Config) GetListQuorum() int {
	switch sCfg.ListQuorum {
	case "reduced":
		return 2
	case "disk":
		// smallest possible value, generally meant for testing.
		return 1
	case "strict":
		return -1
	}
	// Defaults to 3 drives per set, defaults to "optimal" value
	return 3
}

// LookupConfig - lookup api config and override with valid environment settings if any.
func LookupConfig(kvs config.KVS) (cfg Config, err error) {
	// remove this since we have removed this already.
	kvs.Delete(apiReadyDeadline)

	if err = config.CheckValidKeys(config.APISubSys, kvs, DefaultKVS); err != nil {
		return cfg, err
	}

	// Check environment variables parameters
	requestsMax, err := strconv.Atoi(env.Get(EnvAPIRequestsMax, kvs.Get(apiRequestsMax)))
	if err != nil {
		return cfg, err
	}

	if requestsMax < 0 {
		return cfg, errors.New("invalid API max requests value")
	}

	requestsDeadline, err := time.ParseDuration(env.Get(EnvAPIRequestsDeadline, kvs.Get(apiRequestsDeadline)))
	if err != nil {
		return cfg, err
	}

	clusterDeadline, err := time.ParseDuration(env.Get(EnvAPIClusterDeadline, kvs.Get(apiClusterDeadline)))
	if err != nil {
		return cfg, err
	}

	corsAllowOrigin := strings.Split(env.Get(EnvAPICorsAllowOrigin, kvs.Get(apiCorsAllowOrigin)), ",")

	remoteTransportDeadline, err := time.ParseDuration(env.Get(EnvAPIRemoteTransportDeadline, kvs.Get(apiRemoteTransportDeadline)))
	if err != nil {
		return cfg, err
	}

	listQuorum := env.Get(EnvAPIListQuorum, kvs.Get(apiListQuorum))
	switch listQuorum {
	case "strict", "optimal", "reduced", "disk":
	default:
		return cfg, errors.New("invalid value for list strict quorum")
	}

	listLife, err := time.ParseDuration(env.Get(EnvAPIExtendListCacheLife, kvs.Get(apiExtendListCacheLife)))
	if err != nil {
		return cfg, err
	}

	replicationWorkers, err := strconv.Atoi(env.Get(EnvAPIReplicationWorkers, kvs.Get(apiReplicationWorkers)))
	if err != nil {
		return cfg, err
	}

	if replicationWorkers <= 0 {
		return cfg, config.ErrInvalidReplicationWorkersValue(nil).Msg("Minimum number of replication workers should be 1")
	}

	return Config{
		RequestsMax:             requestsMax,
		RequestsDeadline:        requestsDeadline,
		ClusterDeadline:         clusterDeadline,
		CorsAllowOrigin:         corsAllowOrigin,
		RemoteTransportDeadline: remoteTransportDeadline,
		ListQuorum:              listQuorum,
		ExtendListLife:          listLife,
		ReplicationWorkers:      replicationWorkers,
	}, nil
}
