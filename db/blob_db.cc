// Copyright (c) 2023. Chengye YU <yuchengye2013 AT outlook.com>
// SPDX-License-Identifier: BSD-3-Clause

#include "db/blob_db.h"

#include "db/blob_vlog_impl.h"
#include "db/db_iter.h"
#include "db/filename.h"
#include "db/memtable.h"
#include "db/version_set.h"
#include "db/write_batch_internal.h"

#include "leveldb/options.h"
#include "leveldb/status.h"

namespace leveldb {

struct BlobDB::Writer {
  explicit Writer(port::Mutex* mu) : done(false), cv(mu) {}

  WriteOptions options;
  WriteBatch* updates = nullptr;
  WriteCallback* callback = nullptr;
  WriteBatch small;
  ValueBatch large;
  Status status;
  bool done;
  port::CondVar cv;
};

class DivideHandler : public WriteBatch::Handler {
 public:
  DivideHandler() = default;
  ~DivideHandler() override = default;

  void Put(const Slice& key, const Slice& value) override {
    if (value.size() >= threshold) {
      vb->PutBorrowed(key, value);
    } else {
      small->Put(key, value);
    }
  }

  void Delete(const Slice& key) override { small->Delete(key); }

  size_t threshold;
  ValueBatch* vb;
  WriteBatch* small;
};

Status ValueLogImpl::Open(const Options& options, const std::string& dbname,
                          DB* db, ValueLogImpl** vlog) {
  assert(db != nullptr);
  if (vlog != nullptr) {
    *vlog = nullptr;
  }

  ValueLogImpl* vlog_impl = new ValueLogImpl(options, dbname, db);
  vlog_impl->Recover();
  *vlog = vlog_impl;

  return Status::OK();
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
  Writer w(&write_mutex_);
  w.options = options;
  w.updates = updates;
  w.callback = callback;
  Status s = DivideWriteBatch(updates, &w.small, &w.large);
  if (!s.ok()) {
    return s;
  }

  write_mutex_.Lock();
  writers_.push_back(&w);
  while (!w.done && &w != writers_.front()) {
    w.cv.Wait();
  }
  if (w.done) {
    write_mutex_.Unlock();
    return w.status;
  }

  std::vector<Writer*> group(writers_.begin(), writers_.end());
  write_mutex_.Unlock();

  WriteOptions group_options = group.front()->options;
  std::vector<ValueBatch*> batches;
  batches.reserve(group.size());
  for (Writer* writer : group) {
    group_options.sync = group_options.sync || writer->options.sync;
    batches.push_back(&writer->large);
  }

  {
    WriteLock l(&rwlock_);
    s = vlog_->Write(group_options, batches);
  }

  if (s.ok()) {
    for (Writer* writer : group) {
      s = writer->large.ToWriteBatch(&writer->small);
      if (!s.ok()) {
        writer->status = s;
        continue;
      }
      writer->status =
          db_->Write(writer->options, &writer->small, writer->callback);
    }
  } else {
    for (Writer* writer : group) {
      writer->status = s;
    }
  }

  write_mutex_.Lock();
  for (Writer* writer : group) {
    assert(writers_.front() == writer);
    writers_.pop_front();
    writer->done = true;
    writer->cv.Signal();
  }
  if (!writers_.empty()) {
    writers_.front()->cv.Signal();
  }
  write_mutex_.Unlock();
  return w.status;
}

Status BlobDB::DivideWriteBatch(WriteBatch* input, WriteBatch* small,
                                ValueBatch* large) {
  assert(input != nullptr && small != nullptr && large != nullptr);
  DivideHandler handler;
  handler.threshold = options_.blob_value_size_threshold;
  handler.small = small;
  handler.vb = large;
  Status s = input->Iterate(&handler);
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
  handle.DecodeFrom(&input);
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
  if (dbptr != nullptr) {
    *dbptr = nullptr;
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
  options.env->SetPoolBackgroundThreads(
      options.blob_background_read_threads + 1 +
      options.env->GetPoolBackgroundThreads());
  BlobDB* dbWrapper = new BlobDB(options, name, db, vlog);
  if (dbptr != nullptr) {
    *dbptr = dbWrapper;
  }
  return s;
}

void BlobDB::ManualGC(uint64_t number) { vlog_->ManualGC(number); }

Status BlobDB::VLogBGError() {
  MutexLock l(&vlog_->mutex_);
  return vlog_->bg_error_;
}

Status BlobDB::SyncLSM() { return db_->Sync(); }

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