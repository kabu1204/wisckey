// Copyright (c) 2023. Chengye YU <yuchengye2013 AT outlook.com>
// SPDX-License-Identifier: BSD-3-Clause

#include "db/blob_db.h"
#include "db/blob_vlog_impl.h"
#include "db/blob_vlog_version.h"
#include "db/dbformat.h"
#include "db/filename.h"
#include "db/write_batch_internal.h"

#include "leveldb/status.h"

#include "util/sync_point.h"

namespace leveldb {

class ValueLogGCWriteCallback : public WriteCallback {
 public:
  ~ValueLogGCWriteCallback() override = default;
  ValueLogGCWriteCallback(std::string&& key, ValueHandle handle)
      : key_(std::move(key)), handle_(handle) {}

  Status Callback(DB* db) override {
    std::string value;
    ValueType value_type = kTypeDeletion;
    Status s = reinterpret_cast<DBImpl*>(db)->Get(ReadOptions(), key_, &value,
                                                  &value_type);
    if (!s.ok()) {
      result_ = s;
      return result_;
    }
    if (value_type != kTypeValueHandle) {
      result_ = Status::InvalidArgument("KVHandle may be overwritten");
      return result_;
    }

    Slice input(value);
    ValueHandle current;
    s = current.DecodeFrom(&input);
    if (!s.ok() || !input.empty()) {
      result_ = Status::Corruption("invalid value handle during GC rewrite");
      return result_;
    }
    if (current != handle_) {
      result_ = Status::InvalidArgument("KVHandle may be overwritten");
      return result_;
    }

    applied_ = true;
    result_ = Status::OK();
    return result_;
  }

  bool AllowGrouping() const override { return false; }

  Slice key() const { return {key_}; }

  bool applied() const { return applied_; }

  Status result() const { return result_; }

 private:
  friend class ValueLogImpl;

  std::string key_;
  ValueHandle handle_;
  bool applied_{false};
  Status result_;
};

namespace {

constexpr size_t kGCRewriteBatchBytes = 1 << 20;

template <typename Callback>
class ScopeExit {
 public:
  explicit ScopeExit(Callback callback) : callback_(std::move(callback)) {}
  ~ScopeExit() { callback_(); }

  ScopeExit(const ScopeExit&) = delete;
  ScopeExit& operator=(const ScopeExit&) = delete;

 private:
  Callback callback_;
};

Status GetCurrentValueHandle(DBImpl* db, const Slice& key,
                             ValueHandle* current, bool* has_handle) {
  *has_handle = false;
  std::string handle_encoding;
  ValueType value_type = kTypeDeletion;
  Status s = db->Get(ReadOptions(), key, &handle_encoding, &value_type);
  if (s.IsNotFound()) {
    return Status::OK();
  }
  if (!s.ok()) {
    return Status::IOError("[GC] failed to read from LSM", s.ToString());
  }
  if (value_type != kTypeValueHandle) {
    return Status::OK();
  }

  Slice input(handle_encoding);
  s = current->DecodeFrom(&input);
  if (!s.ok() || !input.empty()) {
    return Status::Corruption("invalid value handle during GC collection");
  }
  *has_handle = true;
  return Status::OK();
}

Status IsCurrentValue(DBImpl* db, const Slice& key,
                      const ValueHandle& expected, bool* is_current) {
  ValueHandle current;
  bool has_handle = false;
  Status s = GetCurrentValueHandle(db, key, &current, &has_handle);
  if (!s.ok()) {
    return s;
  }
  *is_current = has_handle && current == expected;
  return Status::OK();
}

uint64_t DiscardPercent(uint64_t discarded, uint64_t total) {
  return total == 0 ? 100 : discarded * 100 / total;
}

}  // namespace

/*
 * TODO: collect several files in one GC
 */
struct GarbageCollection {
  GarbageCollection()
      : number(0),
        obsolete_sequence(0),
        total_size(0),
        total_entries(0),
        discard_size(0),
        discard_entries(0) {}
  ~GarbageCollection() = default;
  uint64_t number;
  uint64_t total_size;
  uint64_t total_entries;
  uint64_t discard_size;
  uint64_t discard_entries;
  SequenceNumber obsolete_sequence;
  Status s;
};

void ValueLogImpl::BGWork(void* vlog) {
  reinterpret_cast<ValueLogImpl*>(vlog)->BGCall();
}

/*
 * Automatic GC visits one read-only vlog per configured interval. Selection
 * currently walks file numbers in order.
 *
 * TODO: The following policies may improve source selection:
 *  1. utilize LSM compaction statistics to select file. (deleted/overwritten
 * value handles)
 *  2. sample blob records to get statistics
 *  3. BG collect is cheaper than rewrite, we can compute scores for each file
 * to guide our GC.
 */
void ValueLogImpl::BGCall() {
  MutexLock l(&mutex_);
  assert(bg_garbage_collection_);
  if (shutdown_.load(std::memory_order_acquire)) {
    // shutting down value log
  } else if (!bg_error_.ok() && !bg_error_.IsNonFatal()) {
    // stop
    Log(options_.info_log, "Fatal BGError: %s", bg_error_.ToString().c_str());
  } else {
    mutex_.Unlock();
    BackgroundGC();
    mutex_.Lock();
  }

  bg_garbage_collection_ = false;
  MaybeScheduleGC();
  bg_work_cv_.SignalAll();
}

void ValueLogImpl::MaybeScheduleGC() {
  mutex_.AssertHeld();
  if (bg_garbage_collection_) {
    // Allow only one GC job at a time.
  } else if (shutdown_.load(std::memory_order_acquire)) {
    // shutting down value log
  } else if (!bg_error_.ok() && !bg_error_.IsNonFatal()) {
    // stop
    Log(options_.info_log, "Fatal BGError: %s", bg_error_.ToString().c_str());
  } else {
    TimePoint now = TimePoint::clock::now();
    std::chrono::duration<int> duration =
        std::chrono::duration_cast<std::chrono::seconds>(now - gc_last_run_);
    const bool periodic_gc_due =
        options_.blob_gc_interval > 0 &&
        duration.count() >= options_.blob_gc_interval;
    if (!manual_gc_files_.empty() || periodic_gc_due) {
      bg_garbage_collection_ = true;
      env_->Schedule(&ValueLogImpl::BGWork, this);
    }
  }
}

void ValueLogImpl::BackgroundGC() {
  TEST_SYNC_POINT("GC.BeforeBackground");

  bool manual = false;
  uint64_t requested_number = 0;
  {
    MutexLock l(&mutex_);
    if (!manual_gc_files_.empty()) {
      manual = true;
      auto request = manual_gc_files_.begin();
      requested_number = *request;
      manual_gc_files_.erase(request);
    }
  }

  rwlock_.WLock();
  GarbageCollection* gc;
  if (manual) {
    gc = PickGC(requested_number);
  } else {
    gc = PickGC(gc_pointer_);
    gc_pointer_ = gc ? gc->number + 1 : 0;
  }
  rwlock_.WUnlock();

  Status s;
  if (!gc) {
    // This is a normal idle state, not an error. Advancing gc_last_run_ below
    // prevents BGCall() from immediately scheduling the same empty scan.
    s = Status::OK();
  } else {
    s = Collect(gc);
    if (s.ok()) {
      TEST_SYNC_POINT("GC.AfterCollect");
      s = Rewrite(gc);
    }
  }

  if (s.ok() || s.IsNonFatal()) {
    // Reclaim files whose last possible LSM snapshot has expired. Run this for
    // idle scans too, so snapshot release does not require another eligible
    // source file or a foreground write before disk space is reclaimed.
    WriteLock l(&rwlock_);
    RemoveObsoleteFiles();
  }

  {
    MutexLock l(&mutex_);
    // Rate-limit complete GC attempts, including empty scans, threshold misses,
    // and non-fatal failures. Manual requests still bypass this timestamp.
    gc_last_run_ = TimePoint::clock::now();
  }

  RecordBGError(std::move(s));
  delete gc;
}

void ValueLogImpl::RecordBGError(Status&& s) {
  MutexLock l(&mutex_);
  bg_error_ = std::move(s);
}

// Pick the first eligible read-only vlog whose file number is at least number.
GarbageCollection* ValueLogImpl::PickGC(uint64_t number) {
  rwlock_.AssertRLockHeld();

  while (true) {
    auto it = ro_files_.lower_bound(number);
    if (it == ro_files_.end()) {
      Log(options_.info_log, "No eligible vlog at or after #%llu",
          static_cast<unsigned long long>(number));
      return nullptr;
    }
    if (obsolete_files_.find(it->first) == obsolete_files_.end()) {
      if (pending_publications_.count(it->first) != 0) {
        deferred_gc_files_.insert(it->first);
        number = it->first + 1;
        continue;
      }
      number = it->first;
      break;
    }
    number = it->first + 1;
  }

  GarbageCollection* gc = new GarbageCollection();
  gc->number = number;
  return gc;
}

void ValueLogImpl::ManualGC(uint64_t number) {
  MutexLock l(&mutex_);
  manual_gc_files_.insert(number);
  MaybeScheduleGC();
}

Status ValueLogImpl::RunGC() {
  // Stop/restart of the periodic timer and the finite source snapshot form one
  // maintenance operation. Concurrent callers must not race on the timer
  // thread or collect the same sources simultaneously.
  std::lock_guard<std::mutex> run_lock(run_gc_mutex_);
  TEST_SYNC_POINT("GC.RunGC.InCriticalSection");

  // Freeze periodic selection while taking the source-file snapshot. This
  // makes a full maintenance pass visit each file that existed at entry at
  // most once, and never loops over replacement files created by the pass.
  StopGCThread();
  auto restart_gc_timer = [this] { StartGCThread(); };
  ScopeExit<decltype(restart_gc_timer)> restart_timer(restart_gc_timer);

  Status result;
  {
    MutexLock l(&mutex_);
    while (bg_garbage_collection_) {
      bg_work_cv_.Wait();
    }
    if (!bg_error_.ok() && !bg_error_.IsNonFatal()) {
      result = bg_error_;
    }
  }
  if (!result.ok()) {
    return result;
  }

  std::vector<uint64_t> sources;
  {
    WriteLock l(&rwlock_);
    sources.reserve(ro_files_.size());
    for (const auto& file : ro_files_) {
      const uint64_t number = file.first;
      if (obsolete_files_.count(number) != 0) {
        continue;
      }
      if (pending_publications_.count(number) != 0) {
        deferred_gc_files_.insert(number);
        continue;
      }
      sources.push_back(number);
    }
  }

  for (uint64_t number : sources) {
    {
      WriteLock l(&rwlock_);
      if (ro_files_.count(number) == 0 || obsolete_files_.count(number) != 0) {
        continue;
      }
      // A rolled file can still have an unpublished handle batch. Never
      // collect it until that writer leaves DBImpl's publication queue.
      if (pending_publications_.count(number) != 0) {
        deferred_gc_files_.insert(number);
        continue;
      }
    }

    ManualGC(number);
    {
      MutexLock l(&mutex_);
      while (bg_garbage_collection_) {
        bg_work_cv_.Wait();
      }
      if (!bg_error_.ok() && !bg_error_.IsNonFatal()) {
        result = bg_error_;
      }
    }
    if (!result.ok()) {
      return result;
    }
  }

  {
    WriteLock l(&rwlock_);
    RemoveObsoleteFiles();
  }
  return Status::OK();
}

Status ValueLogImpl::Collect(GarbageCollection* gc) {
  ReadLock l(&rwlock_);
  assert(gc != nullptr);
  Log(options_.info_log, "Collecting old entries in vlog %llu\n",
      static_cast<unsigned long long>(gc->number));
  Status s;
  uint64_t number = gc->number;
  if (number == 0 || ro_files_.find(number) == ro_files_.end()) {
    return Status::NonFatal("invalid file number", std::to_string(number));
  }

  VLogReaderIterator* iter = reinterpret_cast<VLogReaderIterator*>(
      NewVLogFileIterator(ReadOptions(), number));
  if (iter == nullptr) {
    return Status::NonFatal("invalid file number", std::to_string(number));
  }

  /*
   * Unlock while reading from vlog file
   */
  rwlock_.RUnlock();

  ValueHandle current;
  current.table_ = number;
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    iter->GetValueHandle(&current);

    gc->total_entries++;
    gc->total_size += current.size_;

    bool is_current = false;
    s = IsCurrentValue(db_, iter->key(), current, &is_current);
    if (!s.ok()) {
      break;
    }
    if (!is_current) {
      gc->discard_entries++;
      gc->discard_size += current.size_;
    }
  }
  if (s.ok()) {
    s = iter->status();
  }
  delete iter;

  rwlock_.RLock();
  return s;
}

/*
 * Rewrite will write to ValueLog, it needs external synchronization.
 *
 * Crash consistency:
 *  1. Crash happens before LSM rewrite:
 *      This leaves an untracked .vlog file, it will be marked deleted by GC
 * later.
 *  2. Crash happens before apply BlobVersionEdit:
 *       This also leaves an untracked .vlog file, but the LSM contains
 * ValueHandles pointing to this file. When recovering, we validate and add the
 * untracked file to ro_files.
 */
Status ValueLogImpl::Rewrite(GarbageCollection* gc) {
  Status s;
  assert(gc != nullptr);
  Log(options_.info_log, "Rewriting vlog %llu\n",
      static_cast<unsigned long long>(gc->number));
  const uint64_t size_discard_percent =
      DiscardPercent(gc->discard_size, gc->total_size);
  const uint64_t entry_discard_percent =
      DiscardPercent(gc->discard_entries, gc->total_entries);
  Log(options_.info_log,
      "[GC #%llu] Size based discard ratio: %llu/%llu = %llu%%",
      static_cast<unsigned long long>(gc->number),
      static_cast<unsigned long long>(gc->discard_size),
      static_cast<unsigned long long>(gc->total_size),
      static_cast<unsigned long long>(size_discard_percent));
  Log(options_.info_log,
      "[GC #%llu] Num based discard ratio: %llu/%llu = %llu%%",
      static_cast<unsigned long long>(gc->number),
      static_cast<unsigned long long>(gc->discard_entries),
      static_cast<unsigned long long>(gc->total_entries),
      static_cast<unsigned long long>(entry_discard_percent));
  if (static_cast<int>(size_discard_percent) <
          options_.blob_gc_size_discard_threshold &&
      static_cast<int>(entry_discard_percent) <
          options_.blob_gc_num_discard_threshold) {
    return Status::NonFatal(
        "Discarded entries/size does not reach the threshold");
  }

  if (gc->discard_entries == gc->total_entries) {
    Log(options_.info_log,
        "[GC #%llu] All entries discarded, removing the entire file",
        static_cast<unsigned long long>(gc->number));
    // The records which made this source stale may be nonsync foreground
    // overwrites or deletes. Make their vlog data and LSM WAL durable before
    // the manifest can make the old vlog reclaimable.
    s = SyncLSM();
    if (!s.ok()) {
      return s;
    }
    gc->obsolete_sequence = db_->LatestSequence();
    BlobVersionEdit edit;
    edit.DeleteFile(gc->number, gc->obsolete_sequence);
    WriteLock l(&rwlock_);
    return LogAndApply(&edit);
  }

  /*
   * we create another file instead of directly writing to rwfile_
   */
  AppendableRandomAccessFile* file = nullptr;
  VLogBuilder* builder = nullptr;
  bool builder_closed = false;
  uint64_t number;
  {
    WriteLock l(&rwlock_);
    number = NewFileNumber();
    s = env_->NewAppendableRandomAccessFile(VLogFileName(dbname_, number),
                                            &file);
    if (!s.ok()) {
      return s;
    }
    builder = new VLogBuilder(options_, file, false);
    pending_outputs_.emplace(number);

    Log(options_.info_log, "[GC #%llu] Rewriting to vlog#%llu",
        static_cast<unsigned long long>(gc->number),
        static_cast<unsigned long long>(number));
  }

  // No LSM handle refers to the replacement until it is published below, so
  // all failures during the value-copy phase can safely discard the output.
  auto discard_unpublished_output = [&](Status error) {
    if (builder != nullptr && !builder_closed) {
      builder->Abandon();
      builder_closed = true;
    }
    if (file != nullptr) {
      file->Close();
    }
    delete builder;
    delete file;
    builder = nullptr;
    file = nullptr;
    {
      WriteLock l(&rwlock_);
      pending_outputs_.erase(number);
    }
    env_->RemoveFile(VLogFileName(dbname_, number));
    return error;
  };

  // Re-read the old file and copy only values which are still current. Values
  // are accumulated in a bounded batch; unlike the old implementation this
  // does not retain a whole vlog's live values and WriteCallbacks in memory.
  VLogReaderIterator* old_iter;
  {
    ReadLock l(&rwlock_);
    old_iter = reinterpret_cast<VLogReaderIterator*>(
        NewVLogFileIterator(ReadOptions(), gc->number));
  }
  if (old_iter == nullptr) {
    return discard_unpublished_output(
        Status::NonFatal("invalid file number", std::to_string(gc->number)));
  }

  ValueBatch batch;
  ValueHandle old_handle;
  old_handle.table_ = gc->number;
  auto flush_batch = [&]() {
    if (batch.NumEntries() == 0) {
      return Status::OK();
    }
    batch.Finalize(number, builder->Offset());
    builder->AddBatch(&batch);
    Status result = builder->status();
    TEST_SYNC_POINT("GC.Rewrite.AfterBatch");
    batch = ValueBatch();
    return result;
  };

  uint64_t copied_entries = 0;
  for (old_iter->SeekToFirst(); old_iter->Valid(); old_iter->Next()) {
    if (shutdown_.load(std::memory_order_acquire)) {
      s = Status::IOError("ValueLog shutting down during GC value rewrite");
      break;
    }

    old_iter->GetValueHandle(&old_handle);
    bool is_current = false;
    s = IsCurrentValue(db_, old_iter->key(), old_handle, &is_current);
    if (!s.ok()) {
      break;
    }
    if (!is_current) {
      continue;
    }

    batch.Put(old_iter->key(), old_iter->value());
    ++copied_entries;
    if (batch.size() >= kGCRewriteBatchBytes) {
      s = flush_batch();
      if (!s.ok()) {
        break;
      }
    }
  }
  if (s.ok()) {
    s = old_iter->status();
  }
  if (s.ok()) {
    s = flush_batch();
  }
  delete old_iter;
  if (!s.ok()) {
    return discard_unpublished_output(std::move(s));
  }

  if (copied_entries == 0) {
    discard_unpublished_output(Status::OK());
    Log(options_.info_log,
        "[GC #%llu] All entries became stale during rewrite; removing file",
        static_cast<unsigned long long>(gc->number));
    // Entries can become stale after Collect() due to nonsync foreground
    // writes. Do not durably obsolete their old vlog until those writes are
    // themselves durable.
    s = SyncLSM();
    if (!s.ok()) {
      return s;
    }
    gc->obsolete_sequence = db_->LatestSequence();
    BlobVersionEdit edit;
    edit.DeleteFile(gc->number, gc->obsolete_sequence);
    WriteLock l(&rwlock_);
    return LogAndApply(&edit);
  }

  s = builder->Finish();
  builder_closed = true;
  if (s.ok()) {
    s = file->Sync();
  }
  if (s.ok()) {
    s = file->Close();
  }
  if (!s.ok()) {
    return discard_unpublished_output(std::move(s));
  }

  // add file to ro_files in advance
  VLogFileMeta f;
  f.number = number;
  f.file_size = builder->FileSize();
  {
    rwlock_.WLock();
    ro_files_.emplace(number, f);
    rwlock_.WUnlock();
  }
  delete builder;
  delete file;
  builder = nullptr;
  file = nullptr;

  TEST_SYNC_POINT_MAY_RETURN("GC.Rewrite.AfterValueRewrite", s);

  // 2. rewrite to LSM
  // We disable sync for each record and sync the LSM WAL once after all
  // conditional pointer updates. The replacement vlog was synced first.
  Log(options_.info_log, "[GC #%llu] Rewriting to LSM, vlog#%llu",
      static_cast<unsigned long long>(gc->number),
      static_cast<unsigned long long>(number));
  WriteOptions opt;
  opt.sync = false;

  VLogReaderIterator* new_iter;
  {
    ReadLock l(&rwlock_);
    new_iter = reinterpret_cast<VLogReaderIterator*>(
        NewVLogFileIterator(ReadOptions(), number));
  }
  if (new_iter == nullptr) {
    return Status::IOError("GC replacement vlog is unavailable");
  }

  ValueHandle new_handle;
  new_handle.table_ = number;
  for (new_iter->SeekToFirst(); new_iter->Valid(); new_iter->Next()) {
    if (shutdown_.load(std::memory_order_acquire)) {
      s = Status::IOError("ValueLog shutting down during GC LSM rewrite");
      break;
    }

    ValueHandle current;
    bool has_handle = false;
    s = GetCurrentValueHandle(db_, new_iter->key(), &current, &has_handle);
    if (!s.ok()) {
      break;
    }
    // The selected source file is read-only. If this key still points into it,
    // it must point to the record copied above; otherwise a foreground write
    // has already made the replacement record stale.
    if (!has_handle || current.table_ != gc->number) {
      continue;
    }

    new_iter->GetValueHandle(&new_handle);
    std::string handle_encoding;
    new_handle.EncodeTo(&handle_encoding);
    WriteBatch rewrite;
    WriteBatchInternal::Put(&rewrite, new_iter->key(), handle_encoding,
                            kTypeValueHandle);
    ValueLogGCWriteCallback callback(new_iter->key().ToString(), current);
    s = db_->Write(opt, &rewrite, &callback);
    if (!callback.applied() &&
        (callback.result().IsNotFound() ||
         callback.result().IsInvalidArgument())) {
      // A foreground write won the race after our pre-check. DBImpl reports
      // callback rejection to its caller; for GC this is an expected stale
      // record and must not abort the rest of the file.
      s = Status::OK();
      continue;
    }
    if (!s.ok()) {
      s = Status::IOError("failed to write GC handle to LSM", s.ToString());
      break;
    }
    if (!callback.applied()) {
      s = Status::IOError("failed to validate GC handle",
                          callback.result().ToString());
      break;
    }
  }
  if (s.ok()) {
    s = new_iter->status();
  }
  delete new_iter;
  if (!s.ok()) {
    return s;
  }

  s = SyncLSM();
  if (!s.ok()) {
    return s;
  }

  TEST_SYNC_POINT_MAY_RETURN("GC.Rewrite.AfterLSMRewrite", s);

  // 3. mark old file as obsolete
  gc->obsolete_sequence = db_->LatestSequence();
  BlobVersionEdit edit;
  edit.AddFile(f.number, f.file_size);
  edit.DeleteFile(gc->number, gc->obsolete_sequence);
  rwlock_.WLock();
  s = LogAndApply(&edit);  // we mark the file as obsolete here, it will be
                           // removed from disk at proper time.
  rwlock_.WUnlock();
  return s;
}

}  // namespace leveldb
