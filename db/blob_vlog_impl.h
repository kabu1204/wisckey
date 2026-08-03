// Copyright (c) 2023. Chengye YU <yuchengye2013 AT outlook.com>
// SPDX-License-Identifier: BSD-3-Clause

#ifndef LEVELDB_BLOB_VLOG_IMPL_H
#define LEVELDB_BLOB_VLOG_IMPL_H

#include "db/blob_db.h"
#include "db/blob_vlog_cache.h"
#include "db/blob_vlog_version.h"
#include "db/db_impl.h"
#include "db/log_writer.h"
#include <deque>
#include <map>
#include <set>
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
        number_(number),
        refs_(0) {
    if (file_ != nullptr) {
      builder_ = new VLogBuilder(options, file_, reuse, offset, num_entries);
      status_ = VLogReader::Open(options, file_, offset, &reader_);
    }
  }

  VLogRWFile(const VLogRWFile&) = delete;
  VLogRWFile& operator=(const VLogRWFile&) = delete;

  /*
   * calling Ref() and Unref() concurrently needs external synchronization.
   * See comments in Unref() below.
   */
  void Ref() { ++refs_; }
  void Unref() {
    --refs_;
    assert(refs_ >= 0);
    if (refs_ <= 0) {
      /*
       * There are chances that another thread calls Ref(), and current thread
       * are deleting this. However, in ValueLogImpl, this never happens.
       *
       * ValueLogImpl will call Ref() when creating new VlogRWFile, so before it
       * Unref() (i.e. finish building and creating another one), the refs_ will
       * always be >= 1;
       *
       * Before ValueLogImpl Unref() this, concurrent Ref() and Unref() is safe,
       * because refs_<=0 will always be false, the rwfile can never be deleted
       * by mistake.
       *
       * After ValueLogImpl Unref() this, no more Ref() happens, so it's safe.
       */
      delete this;
    }
  }

  void Write(const ValueBatch* batch) {
    assert(!closed_);
    builder_->AddBatch(batch);
    reader_->IncreaseOffset(builder_->Offset());
  }

  // should be called after Ref()
  void Add(const Slice& key, const Slice& val, ValueHandle* handle) {
    assert(!closed_);
    builder_->Add(key, val, handle);
    reader_->IncreaseOffset(builder_->Offset());
  }

  void Flush() {
    assert(!closed_);
    builder_->Flush();
  }

  Status Sync() {
    assert(!closed_);
    return file_->Sync();
  }

  void Finish() {
    assert(!closed_);
    closed_ = true;
    file_->Sync();
    builder_->Finish();
    file_->Close();
  }

  Iterator* NewIterator(const ReadOptions& options) {
    Ref();
    Iterator* iter = reader_->NewIterator(options);
    iter->RegisterCleanup(&UnrefCleanup, this, nullptr);
    return iter;
  }

  Status Get(const ReadOptions& options, uint32_t offset, uint32_t size,
             std::string* value) {
    return reader_->Get(options, offset, size, value);
  }

  uint32_t Offset() const { return builder_->Offset(); }
  uint32_t FileSize() const { return builder_->FileSize(); }
  uint64_t FileNumber() const { return number_; }
  Status status() const { return builder_->status(); }

 private:
  ~VLogRWFile() {
    assert(closed_);
    delete reader_;
    delete builder_;
    delete file_;
  }

  static void UnrefCleanup(void* arg1, void* arg2) {
    VLogRWFile* p = reinterpret_cast<VLogRWFile*>(arg1);
    p->Unref();
  }

  std::atomic<int> refs_;
  bool closed_;

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

  Status Write(const WriteOptions& options, ValueBatch* batch);
  Status Write(const WriteOptions& options,
               const std::vector<ValueBatch*>& batches);

  Status Get(const ReadOptions& options, const ValueHandle& handle,
             std::string* value);

  //  Status MultiGet(const ReadOptions& options, std::vector<ValueHandle>&
  //  handles,
  //                  std::vector<std::string>* values);

  Status Recover();

  std::string DebugString();

  GarbageCollection* PickGC(uint64_t number);

  void ManualGC(uint64_t number);

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

  Status FinishBuild() EXCLUSIVE_LOCKS_REQUIRED(rwlock_);

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

  void RecordBGError(Status&& s);

  struct ByFileNumber {
    bool operator()(VLogFileMeta* lhs, VLogFileMeta* rhs) const {
      return lhs->number < rhs->number;
    }
  };

  port::RWSpinLock rwlock_;  // protect class members
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
  using TimePoint = std::chrono::time_point<std::chrono::system_clock>;
  std::set<uint64_t> pending_outputs_;  // GC thread is writing to these files
  std::map<uint64_t, SequenceNumber>
      obsolete_files_;  // <file_number, sequence>
  uint64_t gc_pointer_;
  bool manual_gc_;
  uint64_t manual_gc_number;

  port::Mutex mutex_;  // only used to protect following members
  port::CondVar bg_work_cv_
      GUARDED_BY(mutex_);  // synchronize GC thread and foreground threads
  bool bg_garbage_collection_ GUARDED_BY(mutex_);
  TimePoint gc_last_run_ GUARDED_BY(mutex_);
  Status bg_error_ GUARDED_BY(mutex_);
};

}  // namespace leveldb

#endif  // LEVELDB_BLOB_VLOG_IMPL_H
