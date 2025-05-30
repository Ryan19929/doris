// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include <atomic>
#include <map>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

namespace doris {

class WorkloadGroup;

template <typename T>
class AtomicCounter;
using IntCounter = AtomicCounter<int64_t>;

template <typename T>
class AtomicGauge;
using IntGuage = AtomicGauge<int64_t>;

class MetricEntity;
struct MetricPrototype;

class WorkloadGroupMetrics {
public:
    WorkloadGroupMetrics(WorkloadGroup* wg);

    ~WorkloadGroupMetrics();

    void update_cpu_time_nanos(uint64_t delta_cpu_time);

    void update_memory_used_bytes(int64_t memory_used);

    void update_local_scan_io_bytes(std::string path, uint64_t delta_io_bytes);

    void update_remote_scan_io_bytes(uint64_t delta_io_bytes);

    void refresh_metrics();

    uint64_t get_cpu_time_nanos_per_second();

    int64_t get_local_scan_bytes_per_second();

    int64_t get_remote_scan_bytes_per_second();

    int64_t get_memory_used();

    friend class DummyWorkloadGroupTest;

private:
    IntCounter* workload_group_cpu_time_sec {nullptr};           // used for metric
    IntGuage* workload_group_mem_used_bytes {nullptr};           // used for metric
    IntCounter* workload_group_remote_scan_bytes {nullptr};      // used for metric
    IntCounter* workload_group_total_local_scan_bytes {nullptr}; // used for metric
    std::unordered_multimap<std::string, IntCounter*>
            _local_scan_bytes_counter_map; // used for metric

    std::atomic<uint64_t> _cpu_time_nanos {0};
    std::atomic<uint64_t> _last_cpu_time_nanos {0};
    std::atomic<uint64_t> _per_sec_cpu_time_nanos {0}; // used for system table

    std::atomic<uint64_t> _per_sec_local_scan_bytes {0};
    std::atomic<uint64_t> _last_local_scan_bytes {0}; // used for system table

    std::atomic<uint64_t> _per_sec_remote_scan_bytes {0};
    std::atomic<uint64_t> _last_remote_scan_bytes {0}; // used for system table

    std::atomic<uint64_t> _memory_used {0};

    std::shared_ptr<MetricEntity> _entity {nullptr};
    std::vector<std::shared_ptr<MetricEntity>> _io_entity_list;
};

} // namespace doris