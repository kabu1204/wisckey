// Copyright (c) 2023. Chengye YU <yuchengye2013 AT outlook.com>
// SPDX-License-Identifier: BSD-3-Clause

#ifndef LEVELDB_BLOB_DB_H
#define LEVELDB_BLOB_DB_H

#include "db/db_impl.h"
#include <utility>
#include <vector>

#include "leveldb/write_batch.h"

#include "table/format.h"
#include "util/mutexlock.h"

namespace leveldb {

class ValueLogImpl;
class ValueBatch;

class BlobDB : public DB {
 public:
  static Status Open(const Options& options, const std::string& name,
                     BlobDB** dbptr);

  BlobDB(const BlobDB&) = delete;
  BlobDB& operator=(const BlobDB&) = delete;

  ~BlobDB() override;

  Status Put(const WriteOptions& options, const Slice& key,
             const Slice& value) override;

  Status Delete(const WriteOptions& options, const Slice& key) override;

  const Snapshot* GetSnapshot() override;

  void ReleaseSnapshot(const Snapshot* snapshot) override;

  bool GetProperty(const Slice& property, std::string* value) override;

  void GetApproximateSizes(const Range* range, int n, uint64_t* sizes) override;

  void CompactRange(const Slice* begin, const Slice* end) override;

  // Apply the specified updates to the database.
  // Returns OK on success, non-OK on failure.
  // Note: consider setting options.sync = true.
  Status Write(const WriteOptions& options, WriteBatch* updates) override;

  Status Write(const WriteOptions& options, WriteBatch* updates,
               WriteCallback* callback) override;

  Status Get(const ReadOptions& options, const Slice& key,
             std::string* value) override;

  /*
   * Sync LSM, i.e. sync WAL.
   */
  Status SyncLSM();

  void RemoveObsoleteBlob();

  Iterator* NewIterator(const ReadOptions& options) override;

  std::string DebugString();

  void ManualGC(uint64_t number);

  // Sequentially scan all records of .vlog file "number" (no LSM probes).
  Status ScanVLog(uint64_t number, uint64_t* scanned_bytes,
                  uint64_t* num_records);

  void WaitVLogGC();

  Status VLogBGError();

 private:
  friend class DB;
  friend class ValueLogImpl;

  BlobDB(const Options& options, std::string dbname, DB* db, ValueLogImpl* vlog)
      : db_(reinterpret_cast<DBImpl*>(db)),
        vlog_(vlog),
        options_(options),
        dbname_(std::move(dbname)) {}

  Status DivideWriteBatch(WriteBatch* input, WriteBatch* small,
                          ValueBatch* large);

  Options options_;
  std::string dbname_;

  port::RWMutex rwlock_;
  DBImpl* const db_;
  ValueLogImpl* const vlog_;
};

}  // namespace leveldb

#endif  // LEVELDB_BLOB_DB_H
