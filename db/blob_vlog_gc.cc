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
      : key_(key), handle_(handle) {}

  Status Callback(DB* db) override {
    Status s;
    std::string value;
    s = db->Get(ReadOptions(), key_, &value);
    if (!s.ok()) {
      return s;
    }

    Slice input(value);
    ValueHandle current;
    current.DecodeFrom(&input);
    if (current != handle_) {
      return Status::InvalidArgument("KVHandle may be overwritten");
    }

    return Status::OK();
  }

  bool AllowGrouping() const override { return false; }

  Slice key() const { return {key_}; }

 private:
  friend class ValueLogImpl;

  std::string key_;
  ValueHandle handle_;
};

class RewriteLSMHandler : public ValueBatch::Handler {
 public:
  ~RewriteLSMHandler() override = default;

  bool operator()(const Slice& key, const Slice& value,
                  ValueHandle handle) override {
    if (shutdown->load(std::memory_order_acquire)) {
      s = Status::IOError("ValueLog shutting down during GC rewrite");
      return false;
    }

    handle.EncodeTo(&handle_encoding);
    WriteBatchInternal::Put(&iter->first, key, handle_encoding,
                            kTypeValueHandle);
    s = db->Write(opt, &iter->first, &iter->second);
    if (!s.ok()) {
      s = Status::IOError("failed to write to LSM", s.ToString());
      return false;
    }
    ++iter;
    if (iter == end) {
      return false;
    }
    return true;
  }

  std::string handle_encoding;
  std::vector<std::pair<WriteBatch, ValueLogGCWriteCallback>>::iterator iter;
  std::vector<std::pair<WriteBatch, ValueLogGCWriteCallback>>::iterator end;
  WriteOptions opt;
  std::atomic<bool>* shutdown;
  DBImpl* db;
  Status s;
};

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
  ValueBatch value_batch;
  std::vector<std::pair<WriteBatch, ValueLogGCWriteCallback>> rewrites;
  uint32_t total_size;
  uint32_t total_entries;
  uint32_t discard_size;
  uint32_t discard_entries;
  SequenceNumber obsolete_sequence;
  Status s;
};

void ValueLogImpl::BGWork(void* vlog) {
  reinterpret_cast<ValueLogImpl*>(vlog)->BGCall();
}

/*
 * We have no policy on when to GC and which file to GC, we just select one
 * .vlog file polling and sleep for 10 minutes.
 *
 * TODO: Maybe the following policies are helpful:
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
    // allow only one GC thread
    // multiple GC threads is safe in theory, but I don't test :D
  } else if (shutdown_.load(std::memory_order_acquire)) {
    // shutting down value log
  } else if (!bg_error_.ok() && !bg_error_.IsNonFatal()) {
    // stop
    Log(options_.info_log, "Fatal BGError: %s", bg_error_.ToString().c_str());
  } else {
    TimePoint now = std::chrono::system_clock::now();
    std::chrono::duration<int> duration =
        std::chrono::duration_cast<std::chrono::seconds>(now - gc_last_run_);
    if (manual_gc_ || duration.count() > options_.blob_gc_interval) {
      bg_garbage_collection_ = true;
      env_->Schedule(&ValueLogImpl::BGWork, this);
    }
  }
}

void ValueLogImpl::BackgroundGC() {
  rwlock_.WLock();
  GarbageCollection* gc;
  if (manual_gc_) {
    gc = PickGC(manual_gc_number);
    manual_gc_ = false;
  } else {
    gc = PickGC(gc_pointer_);
    gc_pointer_ = gc ? gc->number + 1 : 0;
  }
  rwlock_.WUnlock();

  if (!gc) {
    RecordBGError(Status::NonFatal("Empty GC metadata, skip"));
    return;
  }

  Status s;
  s = Collect(gc);
  if (!s.ok() && !s.IsNonFatal()) {
    goto OUT;
  }

  TEST_SYNC_POINT("GC.AfterCollect");

  s = Rewrite(gc);
  if (!s.ok() && !s.IsNonFatal()) {
    goto OUT;
  }

  {
    MutexLock l(&mutex_);
    gc_last_run_ = std::chrono::system_clock::now();
  }

OUT:
  RecordBGError(std::move(s));
  delete gc;
}

void ValueLogImpl::RecordBGError(Status&& s) {
  MutexLock l(&mutex_);
  bg_error_ = std::move(s);
}

/*
 * We not have any policies :)
 * We just pick the first valid vlog whose file_number >= number.
 */
GarbageCollection* ValueLogImpl::PickGC(uint64_t number) {
  rwlock_.AssertRLockHeld();

  while (true) {
    auto it = ro_files_.lower_bound(number);
    if (it == ro_files_.end()) {
      Log(options_.info_log, "PickGC Restart");
      return nullptr;
    }
    if (obsolete_files_.find(it->first) == obsolete_files_.end()) {
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
  {
    WriteLock l(&rwlock_);
    manual_gc_ = true;
    manual_gc_number = number;
  }

  MutexLock l(&mutex_);
  MaybeScheduleGC();
}

Status ValueLogImpl::Collect(GarbageCollection* gc) {
  ReadLock l(&rwlock_);
  assert(gc != nullptr);
  Log(options_.info_log, "Collecting old entries in vlog %llu\n", gc->number);
  Status s;
  uint64_t number = gc->number;
  if (number >= CurrentFileNumber() || number == 0) {
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

  Slice key;
  ValueType valueType;
  std::string handle_encoding;
  ValueHandle handle, current;
  ValueBatch& vb = gc->value_batch;
  std::vector<std::pair<WriteBatch, ValueLogGCWriteCallback>>& rewrites =
      gc->rewrites;
  current.table_ = number;
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    key = iter->key();

    s = db_->Get(ReadOptions(), key, &handle_encoding, &valueType);
    if (!s.ok() && !s.IsNotFound()) {
      s = Status::IOError("[GC] failed to Get from DBImpl", s.ToString());
      break;
    }

    iter->GetValueHandle(&current);

    gc->total_entries++;
    gc->total_size += current.size_;

    if (valueType == kTypeValueHandle) {
      Slice input(handle_encoding);
      handle.DecodeFrom(&input);
    }
    if (s.IsNotFound() || valueType != kTypeValueHandle || handle != current) {
      gc->discard_entries++;
      gc->discard_size += current.size_;
      continue;
    }

    /*
     * We need to keep the entry.
     */
    vb.Put(key, iter->value());
    rewrites.emplace_back(WriteBatch(),
                          ValueLogGCWriteCallback(key.ToString(), handle));
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
  Log(options_.info_log, "Rewriting vlog %llu\n", gc->number);
  Log(options_.info_log, "[GC #%llu] Size based discard ratio: %u/%u = %d%%",
      gc->number, gc->discard_size, gc->total_size,
      (gc->discard_size * 100 / gc->total_size));
  Log(options_.info_log, "[GC #%llu] Num based discard ratio: %u/%u = %d%%",
      gc->number, gc->discard_entries, gc->total_entries,
      (gc->discard_entries * 100 / gc->total_entries));
  if ((gc->discard_size * 100 / gc->total_size) <
          options_.blob_gc_size_discard_threshold &&
      (gc->discard_entries * 100 / gc->total_entries) <
          options_.blob_gc_num_discard_threshold) {
    return Status::NonFatal(
        "Discarded entries/size does not reach the threshold");
  }

  if (gc->discard_entries == gc->total_entries) {
    Log(options_.info_log,
        "[GC #%llu] All entries discarded, removing the entire file",
        gc->number);
    gc->obsolete_sequence = db_->LatestSequence();
    BlobVersionEdit edit;
    edit.DeleteFile(gc->number, gc->obsolete_sequence);
    WriteLock l(&rwlock_);
    return LogAndApply(&edit);
  }

  /*
   * we create another file instead of directly writing to rwfile_
   */
  AppendableRandomAccessFile* file;
  VLogBuilder* builder;
  uint64_t number;
  {
    WriteLock l(&rwlock_);
    number = NewFileNumber();
    s = env_->NewAppendableRandomAccessFile(VLogFileName(dbname_, number),
                                            &file, options_.blob_max_file_size);
    if (!s.ok()) {
      return s;
    }
    builder = new VLogBuilder(options_, file, false);
    pending_outputs_.emplace(number);

    Log(options_.info_log, "[GC #%llu] Rewriting to vlog#%llu", gc->number,
        number);
  }

  /*
   * The valid values that we are rewriting to the new vlog file, are already
   * persistent. So we need to keep the consistency.
   */
  WriteOptions opt;
  opt.sync = true;

  // 1. write to ValueLog
  gc->value_batch.Finalize(number, 0);
  builder->AddBatch(&gc->value_batch);
  file->Sync();
  builder->Finish();
  file->Close();

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

  TEST_SYNC_POINT_MAY_RETURN("GC.Rewrite.AfterValueRewrite", s);

  // 2. rewrite to LSM
  // we disable sync when writing, and manually sync LSM later
  Log(options_.info_log, "[GC #%llu] Rewriting to LSM, vlog#%llu", gc->number,
      number);
  opt.sync = false;

  RewriteLSMHandler handler;
  handler.shutdown = &shutdown_;
  handler.iter = gc->rewrites.begin();
  handler.end = gc->rewrites.end();
  handler.opt = opt;
  handler.db = db_;

  s = gc->value_batch.Iterate(&handler);
  if (!s.ok() || !handler.s.ok()) {
    return Status::IOError("GC", handler.s.ToString());
  }

  s = db_->Sync();
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