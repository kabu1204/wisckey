// Copyright (c) 2023. Chengye YU <yuchengye2013 AT outlook.com>
// SPDX-License-Identifier: BSD-3-Clause

#include "db/blob_db.h"

#include <algorithm>
#include <limits>

#include "db/blob_vlog_impl.h"
#include "db/db_iter.h"
#include "db/filename.h"
#include "db/memtable.h"
#include "db/version_set.h"
#include "db/write_batch_internal.h"

#include "leveldb/options.h"
#include "leveldb/status.h"

namespace leveldb {

enum class DividedEntryType { kValue, kValueHandle, kDeletion };

struct DividedEntry {
  DividedEntryType type;
  Slice key;
  Slice value;
  size_t handle_index;
};

class DivideHandler final : public WriteBatch::Handler {
 public:
  DivideHandler(size_t threshold, ValueBatch* large,
                std::vector<DividedEntry>* entries)
      : threshold_(threshold), large_(large), entries_(entries) {}
  ~DivideHandler() override = default;

  void Put(const Slice& key, const Slice& value) override {
    if (value.size() >= threshold_) {
      const size_t handle_index = large_->NumEntries();
      large_->PutBorrowed(key, value);
      entries_->push_back(
          {DividedEntryType::kValueHandle, key, Slice(), handle_index});
    } else {
      entries_->push_back({DividedEntryType::kValue, key, value, 0});
    }
  }

  void Delete(const Slice& key) override {
    entries_->push_back({DividedEntryType::kDeletion, key, Slice(), 0});
  }

 private:
  const size_t threshold_;
  ValueBatch* const large_;
  std::vector<DividedEntry>* const entries_;
};

Status ValueLogImpl::Open(const Options& options, const std::string& dbname,
                          DB* db, ValueLogImpl** vlog) {
  if (vlog == nullptr) {
    return Status::InvalidArgument("null value log output");
  }
  *vlog = nullptr;
  if (db == nullptr) {
    return Status::InvalidArgument("null database for value log");
  }

  ValueLogImpl* vlog_impl = new ValueLogImpl(options, dbname, db);
  Status s = vlog_impl->Recover();
  if (!s.ok()) {
    delete vlog_impl;
    return s;
  }
  vlog_impl->StartGCThread();
  *vlog = vlog_impl;
  return s;
}

BlobDB::~BlobDB() {
  delete vlog_;
  delete db_;
}

Status BlobDB::Write(const WriteOptions& options, WriteBatch* updates) {
  return Write(options, updates, nullptr);
}

Status BlobDB::Write(const WriteOptions& options, WriteBatch* updates,
                     WriteCallback* callback) {
  // Nonsync writes retain normal DBImpl concurrency. A sync write forms an
  // exclusive cross-log barrier: it waits for earlier handle publications,
  // blocks later vlog appends, syncs the vlog, then syncs the LSM WAL.
  if (options.sync) {
    WriteLock commit(&vlog_->commit_lock_);
    return WriteInternal(options, updates, callback);
  }
  ReadLock commit(&vlog_->commit_lock_);
  return WriteInternal(options, updates, callback);
}

Status BlobDB::WriteInternal(const WriteOptions& options, WriteBatch* updates,
                             WriteCallback* callback) {
  if (updates == nullptr) {
    return Status::InvalidArgument("null BlobDB write batch");
  }

  ValueBatch large;
  std::vector<DividedEntry> entries;
  entries.reserve(WriteBatchInternal::Count(updates));
  DivideHandler handler(options_.blob_value_size_threshold, &large, &entries);
  Status s = updates->Iterate(&handler);
  if (!s.ok()) {
    return s;
  }

  // The overwhelmingly common delete/small-value path needs no transformed
  // batch and must not touch or sync the value log.
  if (large.NumEntries() == 0) {
    if (options.sync) {
      s = vlog_->SyncVLog();
      if (!s.ok()) return s;
    }
    return db_->Write(options, updates, callback);
  }

  uint64_t publication_token = 0;
  WriteOptions vlog_options = options;
  vlog_options.sync = false;  // the ordered barrier below performs one sync
  s = vlog_->Write(vlog_options, &large, &publication_token);
  if (!s.ok()) {
    return s;
  }

  // Rebuild in the original operation order. The old split-small-then-append-
  // handles implementation changed the result of batches containing repeated
  // keys (for example, Put(large) followed by Delete).
  WriteBatch transformed;
  std::string handle_encoding;
  const std::vector<ValueHandle>& handles = large.Handles();
  for (const DividedEntry& entry : entries) {
    switch (entry.type) {
      case DividedEntryType::kValue:
        transformed.Put(entry.key, entry.value);
        break;
      case DividedEntryType::kValueHandle:
        if (entry.handle_index >= handles.size()) {
          vlog_->ReleasePublication(publication_token);
          return Status::Corruption("missing value handle after vlog write");
        }
        handle_encoding.clear();
        handles[entry.handle_index].EncodeTo(&handle_encoding);
        WriteBatchInternal::Put(&transformed, entry.key, handle_encoding,
                                kTypeValueHandle);
        break;
      case DividedEntryType::kDeletion:
        transformed.Delete(entry.key);
        break;
      }
  }
  if (options.sync) {
    s = vlog_->SyncVLog();
    if (!s.ok()) {
      vlog_->ReleasePublication(publication_token);
      return s;
    }
  }
  DBImpl::WriteState write_state = DBImpl::WriteState::kNotWritten;
  s = db_->WriteWithOutcome(options, &transformed, callback, &write_state);
  if (s.ok() || write_state == DBImpl::WriteState::kNotWritten) {
    vlog_->ReleasePublication(publication_token);
  }
  // Once WAL emission starts, any error can be indeterminate regardless of
  // its Status category: the record may have reached durable storage even
  // though DBImpl did not publish it to the memtable.
  // Conservatively retain the file pin until restart so GC cannot remove a
  // vlog which crash recovery might later reference. Recovery reconstructs
  // the authoritative handle set and naturally drops this in-memory pin.
  return s;
}

/*
 * thread safety analysis:
 * <W_VLOG> Write to VLOG   <W_PTR> Write pointer to LSM
 * <R_VLOG> Read from VLOG  <R_PTR> Read value pointer from LSM
 *
 * Both VLOG and LSM are internally thread-safe themselves (one Writer and
 * multiple Readers).
 *
 * Assuming GC is disabled, <R_VLOG> always return the correct value of the
 * ValueHandle retrieved by <R_PTR>, because we <W_VLOG> before <W_PTR>.
 *
 * When GC is enabled, <R_VLOG> may return the bad value(either NotFound or
 * incorrect), because the corresponding .vlog file maybe deleted between
 * <R_PTR> and <R_VLOG>. We address this issue without extra synchronization,
 * see comments below.
 */
Status BlobDB::Get(const ReadOptions& options, const Slice& key,
                   std::string* value) {
  ValueType valueType;
  Status s = db_->Get(options, key, value, &valueType);
  if (!s.ok() || valueType != kTypeValueHandle) {
    return s;
  }

  ValueHandle handle;
  Slice input(*value);
  s = handle.DecodeFrom(&input);
  if (s.ok() && !input.empty()) {
    s = Status::Corruption("trailing bytes in value-log handle");
  }
  if (!s.ok()) {
    return s;
  }
  s = vlog_->Get(options, handle, value);

  if (s.ok() || options.snapshot != nullptr || !s.IsNotFound()) {
    // most cases
  } else if (!s.IsNotFound()) {
    // maybe a fatal error
  } else if (options.snapshot != nullptr) {
    /*
     * vlog->Get() returns a not found, indicating the target file is not found.
     *
     * When options.snapshot != nullptr, the vlog guarantees the file pointed by
     * handle will not be removed.
     *
     * When options.snapshot == nullptr, the target file maybe removed by vlog
     * because the smallest sequence of DB > the obsolete sequence of the file.
     * (see comments of ValueLogImpl::Collect and
     * ValueLogImpl::RemoveObsoleteFiles)
     *
     * In the later case (which is rare), we GetSnapshot and retry.
     */
  } else {
    ReadOptions opt = options;
    opt.snapshot = db_->GetSnapshot();
    s = Get(opt, key, value);
    db_->ReleaseSnapshot(opt.snapshot);
    return s;
  }
  return s;
}

Status BlobDB::Put(const WriteOptions& options, const Slice& key,
                   const Slice& val) {
  return DB::Put(options, key, val);
}

Status BlobDB::Delete(const WriteOptions& options, const Slice& key) {
  return DB::Delete(options, key);
}

std::string BlobDB::DebugString() {
  std::string result;
  db_->GetProperty("leveldb.stats", &result);
  result += "\n";
  result += vlog_->DebugString();
  return result;
}

static void ReleaseBlobDBIterSnapshot(void* arg1, void* arg2) {
  reinterpret_cast<BlobDB*>(arg1)->ReleaseSnapshot(
      reinterpret_cast<const Snapshot*>(arg2));
}

Iterator* BlobDB::NewIterator(const ReadOptions& options) {
  const Snapshot* snapshot = options.snapshot;
  if (!snapshot) {
    // If user doesn't specify snapshot, we have to get a new snapshot.
    // See comments of DBWrapper::Get.
    snapshot = GetSnapshot();
  }
  SequenceNumber latest_snapshot;
  uint32_t seed;
  Iterator* iter = db_->NewInternalIterator(options, &latest_snapshot, &seed);
  Iterator* blob_iter = NewBlobDBIterator(
      db_, vlog_, options_.env, db_->user_comparator(), iter,
      static_cast<const SnapshotImpl*>(snapshot)->sequence_number(), seed,
      options.blob_prefetch, options_.blob_background_read_threads);

  if (!options.snapshot) {
    // If user doesn't specify snapshot, we need to release the snapshot we got
    // above after the iter is deleted.
    blob_iter->RegisterCleanup(&ReleaseBlobDBIterSnapshot, this,
                               (void*)(snapshot));
  }

  return blob_iter;
}

const Snapshot* BlobDB::GetSnapshot() { return db_->GetSnapshot(); }

void BlobDB::ReleaseSnapshot(const Snapshot* snapshot) {
  db_->ReleaseSnapshot(snapshot);
}

bool BlobDB::GetProperty(const Slice& property, std::string* value) {
  value->clear();
  bool ret = db_->GetProperty(property, value);
  if (ret && property == "leveldb.stats") {
    value->append("\n");
    value->append(vlog_->DebugString());
  }
  return ret;
}

void BlobDB::GetApproximateSizes(const Range* range, int n, uint64_t* sizes) {
  db_->GetApproximateSizes(range, n, sizes);
}

void BlobDB::CompactRange(const Slice* begin, const Slice* end) {
  db_->CompactRange(begin, end);
}

Status BlobDB::Open(const Options& options, const std::string& name,
                    BlobDB** dbptr) {
  if (dbptr == nullptr) {
    return Status::InvalidArgument("null BlobDB output");
  }
  *dbptr = nullptr;
  if (options.blob_max_file_size == 0 ||
      options.blob_max_file_size > std::numeric_limits<uint32_t>::max()) {
    return Status::InvalidArgument("blob_max_file_size must be in [1, 4GiB]");
  }
  if (options.blob_background_read_threads < 0) {
    return Status::InvalidArgument(
        "blob_background_read_threads must not be negative");
  }
  Log(options.info_log,
      "Creating BlobDB, "
      "ValueThreshold = %lu\n\t"
      "BlobMaxFileSize = %lu\n\t"
      "BlobGCSizeDiscardThreshold = %d\n\t"
      "BlobGCNumDiscardThreshold = %d\n\t"
      "BlobGCInterval = %d\n\t"
      "BlobBackgroundReadThreads = %d",
      options.blob_value_size_threshold, options.blob_max_file_size,
      options.blob_gc_size_discard_threshold,
      options.blob_gc_num_discard_threshold, options.blob_gc_interval,
      options.blob_background_read_threads);
  Status s;
  DB* db;
  s = DB::Open(options, name, &db);
  if (!s.ok()) {
    return s;
  }
  ValueLogImpl* vlog;
  s = ValueLogImpl::Open(options, name, db, &vlog);
  if (!s.ok()) {
    delete db;
    return s;
  }
  const int desired_background_threads =
      options.blob_background_read_threads + 2;  // LSM, vlog GC, prefetch
  options.env->SetPoolBackgroundThreads(std::max(
      options.env->GetPoolBackgroundThreads(), desired_background_threads));
  BlobDB* dbWrapper = new BlobDB(options, name, db, vlog);
  *dbptr = dbWrapper;
  return s;
}

void BlobDB::ManualGC(uint64_t number) { vlog_->ManualGC(number); }

Status BlobDB::RunVLogGC() { return vlog_->RunGC(); }

Status BlobDB::VLogBGError() {
  MutexLock l(&vlog_->mutex_);
  return vlog_->bg_error_;
}

Status BlobDB::SyncLSM() { return vlog_->SyncLSM(); }

void BlobDB::RemoveObsoleteBlob() {
  WriteLock l(&vlog_->rwlock_);
  vlog_->RemoveObsoleteFiles();
}

void BlobDB::WaitVLogGC() {
  MutexLock l(&vlog_->mutex_);
  while (vlog_->bg_garbage_collection_) {
    vlog_->bg_work_cv_.Wait();
  }
}

}  // namespace leveldb
