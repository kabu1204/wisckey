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
             uint32_t num_entries = 0);

  VLogRWFile(const VLogRWFile&) = delete;
  VLogRWFile& operator=(const VLogRWFile&) = delete;

  void Ref();
  void Unref();

  // Reserve a non-overlapping range. Reservation is serialized, while the
  // corresponding Write() calls may run concurrently.
  Status Reserve(size_t bytes, uint32_t num_entries, uint32_t* offset);
  Status Write(uint32_t offset, const ValueBatch* batch);

  // Make all successfully completed bytes through end durable. Later
  // disjoint writes are allowed to continue while the sync is in progress.
  Status SyncThrough(uint32_t end);

  // Stop reservations, drain in-flight writes, repair a failed tail, and sync
  // the complete prefix.
  Status Finish();

  Iterator* NewIterator(const ReadOptions& options) {
    Ref();
    Iterator* iter = reader_->NewIterator(options);
    iter->RegisterCleanup(&UnrefCleanup, this, nullptr);
    return iter;
  }

  Status Get(const ReadOptions& options, uint32_t offset, uint32_t size,
             std::string* value) {
    if (reader_ == nullptr) return status();
    return reader_->Get(options, offset, size, value);
  }

  Status status();

  uint32_t Offset();
  uint32_t FileSize();
  uint32_t ReadableSize();
  uint64_t FileNumber() const { return number_; }

 private:
  struct PendingWrite {
    uint32_t offset = 0;
    uint32_t end = 0;
    uint32_t num_entries = 0;
    bool done = false;
    Status status;
  };

  ~VLogRWFile();

  static void UnrefCleanup(void* arg1, void* arg2) {
    VLogRWFile* p = reinterpret_cast<VLogRWFile*>(arg1);
    p->Unref();
  }

  void SaveError(const Status& status);
  Status ResultStatus() EXCLUSIVE_LOCKS_REQUIRED(state_mutex_);
  void CompleteWrite(uint32_t offset, const Status& status);
  void MaybeScheduleBackgroundSync();
  static void BackgroundSync(void* arg);

  std::atomic<int> refs_;
  port::Mutex state_mutex_;
  port::CondVar state_cv_ GUARDED_BY(state_mutex_);
  port::Mutex sync_mutex_;
  bool sealed_ GUARDED_BY(state_mutex_);
  bool closed_ GUARDED_BY(state_mutex_);
  bool file_closed_;
  uint32_t reserved_end_ GUARDED_BY(state_mutex_);
  uint32_t readable_end_ GUARDED_BY(state_mutex_);
  uint32_t durable_end_ GUARDED_BY(state_mutex_);
  uint32_t num_entries_ GUARDED_BY(state_mutex_);
  size_t in_flight_ GUARDED_BY(state_mutex_);
  std::vector<PendingWrite> pending_writes_ GUARDED_BY(state_mutex_);
  uint32_t failed_offset_ GUARDED_BY(state_mutex_);
  Status write_failure_ GUARDED_BY(state_mutex_);
  // File-wide failures (open, sync, truncate, or close) invalidate every
  // range. Positioned-write failures are tracked separately above so an
  // earlier contiguous prefix can still be synced and published.
  Status status_ GUARDED_BY(state_mutex_);
  const uint64_t number_;
  Env* const env_;
  const uint32_t background_sync_step_;
  bool background_sync_active_ GUARDED_BY(state_mutex_) = false;

  AppendableRandomAccessFile* const file_;
  VLogReader* reader_;
};

class ValueLogGCWriteCallback;
struct GarbageCollection;

// Pairwise runtime guard mirrored by formal/maintenance/policy.py. Source file
// zero denotes an empty scan and is never a parallel collection candidate.
inline bool CanRunVLogGCConcurrently(uint64_t lhs_source,
                                     uint64_t rhs_source) {
  return lhs_source != 0 && rhs_source != 0 && lhs_source != rhs_source;
}

/*
 * thread-safe for 1 Add() and N Get()
 */
class ValueLogImpl {
 public:
  struct WriteReservation {
    VLogRWFile* file = nullptr;
    uint64_t file_number = 0;
    uint32_t offset = 0;
    uint32_t end = 0;
    bool publication_pinned = false;
  };

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

  // Reserve in admission order, write outside the reservation mutex, then
  // release the operation reference after LSM publication is resolved.
  Status ReserveWrite(ValueBatch* batch, bool pin_publication,
                      WriteReservation* reservation);
  Status WriteReserved(const WriteReservation& reservation,
                       const ValueBatch* batch);
  void ReleaseReservation(WriteReservation* reservation,
                          bool release_publication);

  // Releases a file pin returned by Write after the corresponding LSM update
  // has either completed or failed.
  void ReleasePublication(uint64_t publication_token, size_t count = 1);

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
  // append mutex keeps the pointer stable while the reference is acquired.
  VLogRWFile* RefCurrentFile() EXCLUSIVE_LOCKS_REQUIRED(append_mutex_);

  bool HasPendingPublication(uint64_t number);

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

  static void BGWork(void* task);

  void BGCall(GarbageCollection* gc, bool periodic, uint64_t job_id);

  void MaybeScheduleGC();

  void ScheduleGC(GarbageCollection* gc, bool periodic);

  Status BackgroundGC(GarbageCollection* gc);

  void StartGCThread();

  void StopGCThread();

  void StopBackgroundWork();

  void GCThread();

  void RecordBGError(uint64_t job_id, const Status& s);

  struct ByFileNumber {
    bool operator()(VLogFileMeta* lhs, VLogFileMeta* rhs) const {
      return lhs->number < rhs->number;
    }
  };

  // Serializes mutation of the current writable file and rollover. Acquire
  // this before rwlock_ when both are needed. Reads only need rwlock_ long
  // enough to pin metadata and therefore do not wait for append/fsync I/O.
  port::Mutex append_mutex_;
  port::RWMutex rwlock_;  // protects class metadata
  const std::string dbname_;
  Options options_;
  Env* const env_;
  DBImpl* const db_;              // LSM
  BlobDB* owner_{nullptr};
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
  // Publication pins are touched once at admission and once after WAL
  // resolution. Keep this hot counter off the broader metadata RW lock.
  port::Mutex publication_mutex_;
  std::map<uint64_t, size_t> pending_publications_
      GUARDED_BY(publication_mutex_);
  std::set<uint64_t> deferred_gc_files_ GUARDED_BY(rwlock_);
  // Sources are reserved before their jobs enter the Env queue. Distinct
  // source files are the runtime form of the Z3-checked parallelism guard.
  std::set<uint64_t> active_gc_files_ GUARDED_BY(rwlock_);
  uint64_t gc_pointer_;

  port::Mutex mutex_;  // only used to protect following members
  port::CondVar bg_work_cv_
      GUARDED_BY(mutex_);  // synchronize GC thread and foreground threads
  size_t bg_gc_jobs_ GUARDED_BY(mutex_) = 0;
  bool periodic_gc_active_ GUARDED_BY(mutex_) = false;
  bool periodic_gc_paused_ GUARDED_BY(mutex_) = false;
  size_t periodic_gc_jobs_ GUARDED_BY(mutex_) = 0;
  uint64_t next_gc_job_id_ GUARDED_BY(mutex_) = 1;
  uint64_t bg_error_job_id_ GUARDED_BY(mutex_) = 0;
  TimePoint gc_last_run_ GUARDED_BY(mutex_);
  Status bg_error_ GUARDED_BY(mutex_);
  // Coalesce duplicate requests without dropping requests for other files.
  // MaybeScheduleGC removes requests as it reserves their source files and
  // fills the configured worker slots.
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
