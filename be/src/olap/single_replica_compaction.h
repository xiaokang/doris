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

#include <vector>

#include "common/status.h"
#include "olap/rowid_conversion.h"
#include "olap/rowset/rowset.h"
#include "olap/tablet.h"
#include "olap/tablet_meta.h"

namespace doris {

class MemTrackerLimiter;

//  SingleReplicaCompaction is used to fetch peer replica compaction result.
class SingleReplicaCompaction {
public:
    SingleReplicaCompaction(const TabletSharedPtr& tablet);
    virtual ~SingleReplicaCompaction();

    Status prepare_compact();
    Status execute_compact();

private:
    Status _pick_rowsets_to_compact();
    void _gc_output_rowset();

    Status _execute_compact_impl();
    Status _do_compaction();
    Status _do_compaction_impl(TBackend& addr, std::string& token);

    Status _fetch_compaction_result(TBackend& addr, std::string& token);
    bool _should_fetch_from_peer(std::vector<Version>& peer_versions);
    Status _make_snapshot(const std::string& ip, int port, TTableId tablet_id,
                          TSchemaHash schema_hash, int timeout_s, const Version& version,
                          std::string* snapshot_path);
    Status _download_files(DataDir* data_dir, const std::string& remote_url_prefix,
                           const std::string& local_path);
    Status _release_snapshot(const std::string& ip, int port, const std::string& snapshot_path);
    Status _finish_clone(const string& clone_dir, const Version& version);

    void _adjust_input_rowset();
    Status _modify_rowsets(); 

    std::shared_ptr<MemTrackerLimiter> _mem_tracker;
    TabletSharedPtr _tablet;
    std::vector<RowsetSharedPtr> _input_rowsets;
    int64_t _input_rowsets_size;
    int64_t _input_row_num;
    int64_t _input_segments_num;
    int64_t _input_index_size;

    RowsetSharedPtr _output_rowset;

    enum SingleReplicaCompactionState { INITED = 0, SUCCESS = 1 };
    SingleReplicaCompactionState _state;

    Version _output_version;
    RowIdConversion _rowid_conversion;


    DISALLOW_COPY_AND_ASSIGN(SingleReplicaCompaction);   
};

} // namespace doris