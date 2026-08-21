// Copyright (c) 2023. Chengye YU <yuchengye2013 AT outlook.com>
// SPDX-License-Identifier: BSD-3-Clause

#ifndef LEVELDB_BLOB_VLOG_IMPL_H
#define LEVELDB_BLOB_VLOG_IMPL_H

#include "db/blob_db.h"
#include "db/blob_vlog_cache.h"
#include "db/blob_vlog_version.h"
#include "db/db_impl.h"
#include "db/log_writer.h"
#include <atomic>
#include <condition_variable>
#include <deque>
#include <map>
#include <mutex>
#include <set>
#include <thread>
#include <unordered_map>
#include <vector>

#include "leveldb/env.h"
#include "leveldb/options.h"
#include "leveldb/status.h"

#include "port/port.h"
#include "table/format.h"
#include "table/vlog.h"

namespace leveldb {

class VLogRWFile {
 public:
  VLogRWFile(const Options& options, AppendableRandomAccessFile* f,
             uint64_t number, bool reuse = false, uint32_t offset = 0,
             uint32_t num_entries = 0)
      : file_(f),
        builder_(nullptr),
        reader_(nullptr),
        closed_(false),
        file_closed_(false),
        number_(number),
        refs_(0) {
    if (file_ == nullptr) {
      status_ = Status::InvalidArgument("null writable value log file");
    } else {
      builder_ = new VLogBuilder(options, file_, reuse, offset, num_entries);
      status_ = VLogReader::Open(options, file_, offset, &reader_);
    }
  }

  VLogRWFile(const VLogRWFile&) = delete;
  VLogRWFile& operator=(const VLogRWFile&) = delete;

  void Ref() { refs_.fetch_add(1, std::memory_order_relaxed); }
  void Unref() {
    // The decrement and last-reference decision must be one atomic operation.
    // Separate `--refs_` and `refs_ == 0` operations let two readers both
    // observe zero and delete the same file wrapper.
    const int previous = refs_.fetch_sub(1, std::memory_order_acq_rel);
    assert(previous > 0);
    if (previous == 1) {
      delete this;
    }
  }

  Status Write(const ValueBatch* batch) {
    if (closed_) {
      return status_.ok() ? Status::IOError("value log file is closed")
                          : status_;
    }
    if (!status_.ok()) return status_;
    builder_->AddBatch(batch);
    SaveError(builder_->status());
    if (!status_.ok()) return status_;
    reader_->IncreaseOffset(builder_->Offset());
    return Status::OK();
  }

  // should be called after Ref()
  Status Add(const Slice& key, const Slice& val, ValueHandle* handle) {
    if (closed_) {
      return status_.ok() ? Status::IOError("value log file is closed")
                          : status_;
    }
    if (!status_.ok()) return status_;
    builder_->Add(key, val, handle);
    SaveError(builder_->status());
    if (!status_.ok()) return status_;
    reader_->IncreaseOffset(builder_->Offset());
    return Status::OK();
  }

  Status Flush() {
    if (closed_) return status_;
    builder_->Flush();
    SaveError(builder_->status());
    return status_;
  }

  Status Sync() {
    if (closed_) return status_;
    if (!status_.ok()) return status_;
    SaveError(file_->Sync());
    return status_;
  }

  Status Finish() {
    if (closed_) return status_;
    if (builder_ != nullptr) SaveError(builder_->Finish());
    if (file_ != nullptr) {
      SaveError(file_->Sync());
      // Published files are finished with both their metadata-owner reference
      // and an operation reference held. That forces physical Close() to the
      // destructor, after every reader reference is gone. The <= 1 case is
      // only for unpublished construction-error cleanup, where no reader can
      // acquire a reference.
      if (refs_.load(std::memory_order_acquire) <= 1) {
        SaveError(file_->Close());
        file_closed_ = true;
      }
    }
    closed_ = true;
    return status_;
  }

  Iterator* NewIterator(const ReadOptions& options) {
    Ref();
    Iterator* iter = reader_->NewIterator(options);
    iter->RegisterCleanup(&UnrefCleanup, this, nullptr);
    return iter;
  }

  Status Get(const ReadOptions& options, uint32_t offset, uint32_t size,
             std::string* value) {
    if (reader_ == nullptr) return status_;
    return reader_->Get(options, offset, size, value);
  }

  Status status() const { return status_; }

  uint32_t Offset() const { return builder_->Offset(); }
  uint32_t FileSize() const { return builder_->FileSize(); }
  uint64_t FileNumber() const { return number_; }

 private:
  ~VLogRWFile() {
    assert(closed_);
    delete reader_;
    delete builder_;
    if (file_ != nullptr && !file_closed_) {
      (void)file_->Close();
      file_closed_ = true;
    }
    delete file_;
  }

  static void UnrefCleanup(void* arg1, void* arg2) {
    VLogRWFile* p = reinterpret_cast<VLogRWFile*>(arg1);
    p->Unref();
  }

  void SaveError(const Status& status) {
    if (status_.ok() && !status.ok()) {
      status_ = status;
    }
  }

  std::atomic<int> refs_;
  bool closed_;
  bool file_closed_;

  Status status_;
  uint64_t number_;

  AppendableRandomAccessFile* const
      file_;              // the file that currently being built
  VLogBuilder* builder_;  // associated with rw_file_
  VLogReader* reader_;    // associated with rw_file_
};

class ValueLogGCWriteCallback;
struct GarbageCollection;

/*
 * thread-safe for 1 Add() and N Get()
 */
class ValueLogImpl {
 public:
  static Status Open(const Options& options, const std::string& dbname, DB* db,
                     ValueLogImpl** vlog);

  virtual ~ValueLogImpl();

  ValueLogImpl(const ValueLogImpl&) = delete;
  ValueLogImpl& operator=(const ValueLogImpl&) = delete;

  /*
   * concurrent Put() is unsafe
   */
  Status Put(const WriteOptions& options, const Slice& key, const Slice& value,
             ValueHandle* handle);

  Status Write(const WriteOptions& options, ValueBatch* batch,
               uint64_t* publication_token = nullptr);

  // Releases a file pin returned by Write after the corresponding LSM update
  // has either completed or failed.
  void ReleasePublication(uint64_t publication_token);

  Status SyncVLog();

  // Atomically orders value-log durability before LSM WAL durability.
  Status SyncLSM();

  Status Get(const ReadOptions& options, const ValueHandle& handle,
             std::string* value);

  //  Status MultiGet(const ReadOptions& options, std::vector<ValueHandle>&
  //  handles,
  //                  std::vector<std::string>* values);

  Status Recover();

  std::string DebugString();

  GarbageCollection* PickGC(uint64_t number);

  void ManualGC(uint64_t number);

  Status RunGC();

  void RemoveObsoleteFiles() EXCLUSIVE_LOCKS_REQUIRED(rwlock_);

 private:
  friend class BlobDB;

  Status GetLocked(const ReadOptions& options, VLogRWFile* rwfile,
                   const ValueHandle& handle, std::string* value)
      SHARED_LOCKS_REQUIRED(rwlock_);

  explicit ValueLogImpl(const Options& options, const std::string& dbname,
                        DB* db);

  uint64_t NewFileNumber() EXCLUSIVE_LOCKS_REQUIRED(rwlock_) {
    return ++vlog_file_number_;
  }

  void MarkFileNumberUsed(uint64_t number) EXCLUSIVE_LOCKS_REQUIRED(rwlock_) {
    if (number > CurrentFileNumber()) {
      vlog_file_number_ = number;
    }
  }

  uint64_t CurrentFileNumber() const { return vlog_file_number_; }

  Status FinishBuild(VLogRWFile* finished_file)
      EXCLUSIVE_LOCKS_REQUIRED(append_mutex_);

  // Returns the current writable file with one caller-owned reference. The
  // reference lets append/sync I/O proceed after the metadata lock is dropped.
  VLogRWFile* RefCurrentFile();

  Status NewVLogBuilder() EXCLUSIVE_LOCKS_REQUIRED(rwlock_);

  Iterator* NewVLogFileIterator(const ReadOptions& options, uint64_t number);

  Status WriteSnapshot(log::Writer* log) EXCLUSIVE_LOCKS_REQUIRED(rwlock_);

  Status LogAndApply(BlobVersionEdit* edit) EXCLUSIVE_LOCKS_REQUIRED(rwlock_);

  Status ValidateAndTruncate(uint64_t number, uint64_t* offset,
                             uint64_t* num_entries, uint64_t* file_size);

  Status CheckBlobDBExistence();

  Status NewBlobDB();

  Status Collect(GarbageCollection* gc);

  Status Rewrite(GarbageCollection* gc);

  static void BGWork(void* vlog);

  void BGCall();

  void MaybeScheduleGC();

  void BackgroundGC();

  void StartGCThread();

  void StopGCThread();

  void GCThread();

  void RecordBGError(Status&& s);

  struct ByFileNumber {
    bool operator()(VLogFileMeta* lhs, VLogFileMeta* rhs) const {
      return lhs->number < rhs->number;
    }
  };

  // Serializes mutation of the current writable file and rollover. Acquire
  // this before rwlock_ when both are needed. Reads only need rwlock_ long
  // enough to pin metadata and therefore do not wait for append/fsync I/O.
  port::Mutex append_mutex_;
  port::RWMutex rwlock_;      // protects class metadata
  port::RWMutex commit_lock_;  // orders foreground publication and sync
  const std::string dbname_;
  Options options_;
  Env* const env_;
  DBImpl* const db_;              // LSM
  uint64_t vlog_file_number_{0};  // current maximum file number
  uint64_t manifest_number_{0};
  std::atomic<bool> shutdown_;

  /*
   * MANIFEST
   */
  WritableFile* manifest_file_;
  log::Writer* manifest_log_;

  /*
   * Read & Write VLOG
   */
  VLogRWFile* rwfile_;
  std::map<uint64_t, VLogFileMeta> ro_files_
      GUARDED_BY(rwlock_);  // read-only vlog files
  VLogCache* const vlog_cache_{nullptr};

  /*
   * Garbage collection
   */
  using TimePoint = std::chrono::time_point<std::chrono::steady_clock>;
  // Files being produced by GC or whose manifest publication had an
  // indeterminate failure. RemoveObsoleteFiles must retain them.
  std::set<uint64_t> pending_outputs_ GUARDED_BY(rwlock_);
  std::map<uint64_t, SequenceNumber>
      obsolete_files_;  // <file_number, sequence>
  std::map<uint64_t, size_t> pending_publications_ GUARDED_BY(rwlock_);
  std::set<uint64_t> deferred_gc_files_ GUARDED_BY(rwlock_);
  uint64_t gc_pointer_;

  port::Mutex mutex_;  // only used to protect following members
  port::CondVar bg_work_cv_
      GUARDED_BY(mutex_);  // synchronize GC thread and foreground threads
  bool bg_garbage_collection_ GUARDED_BY(mutex_);
  TimePoint gc_last_run_ GUARDED_BY(mutex_);
  Status bg_error_ GUARDED_BY(mutex_);
  // Coalesce duplicate requests without dropping requests for other files.
  // BackgroundGC removes one entry at a time and BGCall reschedules while the
  // set remains non-empty.
  std::set<uint64_t> manual_gc_files_ GUARDED_BY(mutex_);

  // A dedicated lightweight timer triggers GC scheduling. Actual GC work
  // continues to run in the Env pool, so waiting for the next interval never
  // occupies one of its workers.
  std::mutex gc_timer_mutex_;
  std::condition_variable gc_timer_cv_;
  std::thread gc_timer_thread_;
  bool gc_timer_shutdown_{false};
  std::mutex run_gc_mutex_;
};

}  // namespace leveldb

#endif  // LEVELDB_BLOB_VLOG_IMPL_H
