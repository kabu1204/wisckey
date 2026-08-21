// Copyright (c) 2023. Chengye YU <yuchengye2013 AT outlook.com>
// SPDX-License-Identifier: BSD-3-Clause

#include "db/blob_vlog_impl.h"

#include "db/filename.h"
#include "db/log_reader.h"
#include "db/log_writer.h"
#include "db/write_batch_internal.h"

#include "port/port.h"
#include "util/mutexlock.h"

namespace leveldb {

ValueLogImpl::ValueLogImpl(const Options& options, const std::string& dbname,
                           DB* db)
    : env_(options.env),
      db_(reinterpret_cast<DBImpl*>(db)),
      options_(options),
      dbname_(dbname),
      vlog_cache_(new VLogCache(dbname_, options, options.blob_max_open_files)),
      shutdown_(false),
      rwfile_(nullptr),
      manifest_file_(nullptr),
      manifest_log_(nullptr),
      vlog_file_number_(0),
      manifest_number_(0),
      bg_garbage_collection_(false),
      bg_work_cv_(&mutex_),
      gc_pointer_(0),
      gc_last_run_(TimePoint::clock::now()) {}

Status ValueLogImpl::Put(const WriteOptions& options, const Slice& key,
                         const Slice& value, ValueHandle* handle) {
  if (handle == nullptr) {
    return Status::InvalidArgument("null value handle output");
  }
  ValueBatch batch;
  batch.Put(key, value);
  Status s = Write(options, &batch);
  if (s.ok()) {
    *handle = batch.handles_[0];
  }
  return s;
}

Status ValueLogImpl::Write(const WriteOptions& options, ValueBatch* batch,
                           uint64_t* publication_token) {
  if (publication_token != nullptr) {
    *publication_token = 0;
  }
  if (batch == nullptr) {
    return Status::InvalidArgument("null value batch");
  }

  MutexLock append_lock(&append_mutex_);
  VLogRWFile* file = RefCurrentFile();
  if (file == nullptr) {
    return Status::IOError("value log has no writable file");
  }

  // Keep ordinary files within the configured target. An individually large
  // batch still gets its own oversized file rather than forcing an empty
  // rollover loop.
  const uint64_t current_size = file->FileSize();
  if (current_size > 0 &&
      batch->size() > options_.blob_max_file_size -
                          std::min<uint64_t>(current_size,
                                             options_.blob_max_file_size)) {
    Status s = FinishBuild(file);
    file->Unref();
    if (!s.ok()) {
      return s;
    }
    file = RefCurrentFile();
    if (file == nullptr) {
      return Status::IOError("value log has no writable file after rollover");
    }
  }

  batch->Finalize(file->FileNumber(), file->Offset());
  const uint64_t file_number = file->FileNumber();
  Status s = file->Write(batch);
  if (!s.ok()) {
    file->Unref();
    return s;
  }
  if (options.sync) {
    s = file->Sync();
    if (!s.ok()) {
      file->Unref();
      return s;
    }
  }

  // Pin before rollover can schedule GC. The BlobDB caller releases this only
  // after its corresponding handle batch has left DBImpl's writer queue.
  if (publication_token != nullptr) {
    WriteLock metadata_lock(&rwlock_);
    ++pending_publications_[file_number];
  }
  if (file->FileSize() > options_.blob_max_file_size) {
    s = FinishBuild(file);
  }
  if (!s.ok() && publication_token != nullptr) {
    WriteLock metadata_lock(&rwlock_);
    auto it = pending_publications_.find(file_number);
    assert(it != pending_publications_.end() && it->second > 0);
    if (--it->second == 0) pending_publications_.erase(it);
  } else if (publication_token != nullptr) {
    *publication_token = file_number;
  }
  file->Unref();
  return s;
}

VLogRWFile* ValueLogImpl::RefCurrentFile() {
  ReadLock metadata_lock(&rwlock_);
  VLogRWFile* file = rwfile_;
  if (file != nullptr) {
    file->Ref();
  }
  return file;
}

void ValueLogImpl::ReleasePublication(uint64_t publication_token) {
  if (publication_token == 0) return;

  bool retry_deferred_gc = false;
  {
    WriteLock lock(&rwlock_);
    auto it = pending_publications_.find(publication_token);
    assert(it != pending_publications_.end() && it->second > 0);
    if (--it->second == 0) {
      pending_publications_.erase(it);
      retry_deferred_gc = deferred_gc_files_.erase(publication_token) != 0;
    }
  }
  if (retry_deferred_gc) {
    ManualGC(publication_token);
  }
}

Status ValueLogImpl::SyncVLog() {
  MutexLock append_lock(&append_mutex_);
  VLogRWFile* file = RefCurrentFile();
  if (file == nullptr) {
    return Status::IOError("value log has no writable file");
  }
  Status s = file->Sync();
  file->Unref();
  return s;
}

Status ValueLogImpl::SyncLSM() {
  WriteLock commit(&commit_lock_);
  Status s = SyncVLog();
  if (s.ok()) {
    s = db_->Sync();
  }
  return s;
}

Status ValueLogImpl::Get(const ReadOptions& options, const ValueHandle& handle,
                         std::string* value) {
  ReadLock l(&rwlock_);
  if (handle.table_ == 0 || handle.table_ > CurrentFileNumber()) {
    return Status::InvalidArgument("invalid value log number");
  }

  VLogRWFile* rwfile = rwfile_;
  if (rwfile) rwfile->Ref();

  Status s = GetLocked(options, rwfile, handle, value);

  if (rwfile) rwfile->Unref();
  return s;
}

/*
 * TODO: consider using NewVLogFileIterator to make code cleaner
 */
Status ValueLogImpl::GetLocked(const ReadOptions& options, VLogRWFile* rwfile,
                               const ValueHandle& handle, std::string* value) {
  rwlock_.AssertRLockHeld();
  Cache::Handle* cache_handle = nullptr;

  if (!rwfile || handle.table_ != rwfile->FileNumber()) {
    /*
     * Get from read only files
     */
    auto it = ro_files_.find(handle.table_);
    if (it != ro_files_.end()) {
      if (handle.offset_ >= it->second.file_size ||
          handle.size_ > it->second.file_size - handle.offset_) {
        return Status::InvalidArgument("value log offset is out of limit");
      }
      Status s = vlog_cache_->FindReader(handle.table_, it->second.file_size,
                                         &cache_handle);
      if (!s.ok()) {
        return s;
      }
    } else {
      return Status::NotFound("value log number not exists");
    }
  } else {
    /*
     * Get from rwfile
     */
    // The reader's atomically published limit is checked by Get() below.
  }

  // The cache handle or rwfile reference pins the backing file while the
  // metadata lock is dropped for I/O.
  rwlock_.RUnlock();
  Status s;
  if (cache_handle != nullptr) {
    s = vlog_cache_->Get(options, cache_handle, handle.offset_, handle.size_,
                         value);
    vlog_cache_->Release(cache_handle);
  } else {
    s = rwfile->Get(options, handle.offset_, handle.size_, value);
  }
  rwlock_.RLock();
  return s;
}

/*
 * 1. A process crash before syncing won't hurt. The filesystem will flush the
 * dirty page to the disk.
 * 2. An OS crash before DB::Put() return won't hurt.
 * 3. An OS crash between VLog::Put() and LSM::Put() won't hurt.
 * 3. An OS crash after DB::Put(sync=true) won't hurt.
 * 5. An OS crash after DB::Put(sync=false) will cause last few records lost.
 */
Status ValueLogImpl::Recover() {
  WriteLock l(&rwlock_);
  Status s;
  std::vector<std::string> fnames;
  uint64_t number;
  uint64_t latest_rw_file = 0;
  bool has_latest_rw_file = false;
  FileType type;
  Log(options_.info_log, "Recovering ValueLog");

  struct LogReporter : public log::Reader::Reporter {
    Status* status;
    void Corruption(size_t bytes, const Status& s) override {
      if (this->status->ok()) *this->status = s;
    }
  };

  // check existence
  if (!env_->FileExists(dbname_)) {
    s = env_->CreateDir(dbname_);
    if (!s.ok()) return s;
  }
  s = CheckBlobDBExistence();
  if (!s.ok()) {
    return s;
  }

  // Read current manifest filename from VLOG-CURRENT
  std::string current;
  s = ReadFileToString(env_, VLogCurrentFileName(dbname_), &current);
  if (!s.ok()) {
    return s;
  }
  if (current.empty() || current[current.size() - 1] != '\n') {
    return Status::Corruption("VLOG-CURRENT file does not end with newline");
  }
  current.resize(current.size() - 1);

  // Read manifest
  Log(options_.info_log, "VLOG-CURRENT: %s\n", current.c_str());
  std::string manifest_name = dbname_ + "/" + current;
  if (env_->FileExists(manifest_name)) {
    SequentialFile* file = nullptr;
    s = env_->NewSequentialFile(manifest_name, &file);
    if (!s.ok()) {
      return s;
    }
    LogReporter reporter;
    reporter.status = &s;
    log::Reader reader(file, &reporter, true, 0);

    Slice record;
    std::string scratch;
    while (reader.ReadRecord(&record, &scratch) && s.ok()) {
      BlobVersionEdit edit;
      s = edit.DecodeFrom(record);
      if (!s.ok()) {
        break;
      }
      if (edit.has_next_file_number_) {
        if (edit.next_file_number_ == 0) {
          s = Status::Corruption("invalid next value log file number");
          break;
        }
        vlog_file_number_ = edit.next_file_number_ - 1;
      }

      if (edit.has_new_rw_file_) {
        if (edit.new_rw_file_ > vlog_file_number_) {
          s = Status::Corruption("invalid writable value log file number");
          break;
        }
        latest_rw_file = edit.new_rw_file_;
        has_latest_rw_file = true;
      }

      for (auto f : edit.new_files_) {
        if (ro_files_.count(f.number) != 0 ||
            f.number > CurrentFileNumber()) {
          s = Status::Corruption("invalid value log file metadata");
          break;
        }
        ro_files_.emplace(f.number, f);
      }
      if (!s.ok()) break;

      for (auto p : edit.deleted_files_) {
        if (obsolete_files_.count(p.first) != 0 ||
            ro_files_.count(p.first) == 0) {
          s = Status::Corruption("invalid deleted value log metadata");
          break;
        }
        obsolete_files_.emplace(p.first, p.second);
      }
    }

    delete file;
    file = nullptr;
    if (!s.ok()) {
      return s;
    }

  } else {
    return Status::Corruption("value log manifest is missing", manifest_name);
  }

  // scan over all the .vlog files
  s = env_->GetChildren(dbname_, &fnames);
  if (!s.ok()) {
    return s;
  }
  std::set<uint64_t> found_files;
  for (std::string& filename : fnames) {
    if (ParseFileName(filename, &number, &type) && type == kVLogFile) {
      if (ro_files_.count(number) == 0 && latest_rw_file != number) {
        /*
         * This may happen in case of database crash during GC rewriting.
         * We add the file to the ro_files.
         */
        Log(options_.info_log, "Untracked vlog file: %s", filename.c_str());
        uint64_t offset, num_entries, file_size;
        s = ValidateAndTruncate(number, &offset, &num_entries, &file_size);
        if (!s.ok()) {
          return s;
        }

        Log(options_.info_log, "Adding untracked vlog file %s to ro_files",
            filename.c_str());

        VLogFileMeta f;
        f.number = number;
        f.file_size = offset;

        ro_files_.emplace(number, f);
        found_files.emplace(number);
        MarkFileNumberUsed(number);
        continue;
      }

      found_files.emplace(number);

      /*
       * check file size
       */
      uint64_t file_size;
      s = env_->GetFileSize(DBFilePath(dbname_, filename), &file_size);
      if (!s.ok()) {
        return s;
      }
      if (number != latest_rw_file) {
        const uint64_t expected_size = ro_files_.find(number)->second.file_size;
        if (file_size < expected_size) {
          return Status::Corruption("value log file is truncated",
                                    DBFilePath(dbname_, filename));
        }
        if (file_size > expected_size) {
          // Older mmap writers left their unused capacity reflected in the
          // physical size until the last reader released the file.
          s = env_->TruncateFile(DBFilePath(dbname_, filename), expected_size);
          if (!s.ok()) return s;
        }
      }
      Log(options_.info_log, "filename %s: ok", filename.c_str());
    }
  }

  /*
   * Allocate manifest file number.
   * For simplicity, we do not reuse MANIFEST file
   */
  manifest_number_ = NewFileNumber();

  // check already deleted files and lost files
  for (auto it = ro_files_.begin(); it != ro_files_.end();) {
    if (found_files.count(it->first)) {
      it++;
      continue;
    }
    if (obsolete_files_.count(it->first)) {
      /*
       * The to-be-delete file is already removed from disk.
       */

      obsolete_files_.erase(it->first);
      Log(options_.info_log,
          "Not found obsolete file %s, it's removed by previous GC\n",
          VLogFileName(dbname_, it->first).c_str());
    } else {
      Log(options_.info_log, "Lost file: %s\n",
          VLogFileName(dbname_, it->first).c_str());
      return Status::Corruption(
          "BlobDB corruption", "Lost file " + VLogFileName(dbname_, it->first));
    }
    it = ro_files_.erase(it);
  }

  // try to reuse the latest rw file
  uint64_t offset = 0, num_entries = 0, file_size = 0;
  bool reuse = false;
  AppendableRandomAccessFile* file;
  if (has_latest_rw_file) {
    std::string filename(VLogFileName(dbname_, latest_rw_file));
    s = ValidateAndTruncate(latest_rw_file, &offset, &num_entries, &file_size);
    if (!s.ok()) {
      return s;
    }

    if (offset <= (options_.blob_max_file_size / 2)) {
      /*
       * reuse the .vlog file
       */
      Log(options_.info_log, "Reusing %s, ValidSize/FileSize = %llu/%llu",
          filename.c_str(), static_cast<unsigned long long>(offset),
          static_cast<unsigned long long>(file_size));
      reuse = true;
    }
  }

  if (!reuse) {
    offset = 0;
    num_entries = 0;
    BlobVersionEdit edit;
    if (has_latest_rw_file) {
      edit.AddFile(latest_rw_file, file_size);
    }
    edit.NewRWFile(NewFileNumber());
    s = LogAndApply(&edit);
    if (!s.ok()) {
      return s;
    }
  } else {
    s = env_->NewAppendableRandomAccessFile(
        VLogFileName(dbname_, latest_rw_file), &file);
    if (!s.ok()) {
      return s;
    }
    VLogRWFile* reused_file = new VLogRWFile(
        options_, file, latest_rw_file, true, offset, num_entries);
    s = reused_file->status();
    if (!s.ok()) {
      reused_file->Finish();
      reused_file->Ref();
      reused_file->Unref();
      return s;
    }
    reused_file->Ref();
    rwfile_ = reused_file;

    /*
     * We log an empty BlobVersionEdit to create the new manifest file, and
     * WriteSnapshot to the new manifest, to avoid losing the snapshot from the
     * last manifest file.
     */
    BlobVersionEdit edit;
    s = LogAndApply(&edit);
    if (!s.ok()) {
      return s;
    }
  }

  RemoveObsoleteFiles();
  return s;
}

ValueLogImpl::~ValueLogImpl() {
  StopGCThread();

  mutex_.Lock();
  shutdown_.store(true, std::memory_order_release);
  while (bg_garbage_collection_) {
    bg_work_cv_.Wait();
  }
  mutex_.Unlock();

  {
    MutexLock append_lock(&append_mutex_);
    VLogRWFile* file = RefCurrentFile();
    if (file != nullptr) {
      {
        WriteLock metadata_lock(&rwlock_);
        assert(rwfile_ == file);
        rwfile_ = nullptr;
      }
      file->Finish();
      // Release both the reference formerly owned by rwfile_ and our local
      // operation reference. Existing readers keep the wrapper alive until
      // their I/O has completed.
      file->Unref();
      file->Unref();
    }
  }

  delete manifest_log_;
  delete manifest_file_;
  delete vlog_cache_;
}

void ValueLogImpl::StartGCThread() {
  if (options_.blob_gc_interval <= 0) {
    return;
  }

  std::lock_guard<std::mutex> lock(gc_timer_mutex_);
  if (gc_timer_thread_.joinable()) {
    return;
  }
  gc_timer_shutdown_ = false;
  gc_timer_thread_ = std::thread(&ValueLogImpl::GCThread, this);
}

void ValueLogImpl::StopGCThread() {
  {
    std::lock_guard<std::mutex> lock(gc_timer_mutex_);
    gc_timer_shutdown_ = true;
  }
  gc_timer_cv_.notify_all();
  if (gc_timer_thread_.joinable()) {
    gc_timer_thread_.join();
  }
}

void ValueLogImpl::GCThread() {
  std::unique_lock<std::mutex> timer_lock(gc_timer_mutex_);
  const std::chrono::seconds interval(options_.blob_gc_interval);
  while (!gc_timer_shutdown_) {
    if (gc_timer_cv_.wait_for(timer_lock, interval,
                              [this] { return gc_timer_shutdown_; })) {
      break;
    }

    timer_lock.unlock();
    mutex_.Lock();
    if (!shutdown_.load(std::memory_order_acquire)) {
      MaybeScheduleGC();
    }
    mutex_.Unlock();
    timer_lock.lock();
  }
}

Status ValueLogImpl::FinishBuild(VLogRWFile* finished_file) {
  append_mutex_.AssertHeld();
  BlobVersionEdit edit;
  if (finished_file == nullptr) {
    return Status::IOError("value log has no writable file");
  }

  // The append mutex prevents further mutation, and the caller's reference
  // keeps the file alive. Finish the potentially expensive file sync without
  // holding the metadata lock, so reads from every other vlog remain live.
  Status s = finished_file->Finish();
  if (!s.ok()) {
    return s;
  }

  edit.AddFile(finished_file->FileNumber(), finished_file->FileSize());
  {
    WriteLock metadata_lock(&rwlock_);
    if (rwfile_ != finished_file) {
      return Status::IOError("writable value log changed during rollover");
    }
    edit.NewRWFile(NewFileNumber());
    s = LogAndApply(&edit);
    if (!s.ok()) {
      return s;
    }
  }

  // Drop the reference formerly owned by rwfile_. The caller still pins the
  // finished file until it has consumed the result of this rollover.
  finished_file->Unref();

  mutex_.Lock();
  MaybeScheduleGC();
  mutex_.Unlock();
  return Status::OK();
}

Status ValueLogImpl::NewVLogBuilder() {
  rwlock_.AssertWLockHeld();
  Status s;
  uint64_t number = NewFileNumber();
  BlobVersionEdit edit;
  edit.NewRWFile(number);
  s = LogAndApply(&edit);
  return s;
}

Status ValueLogImpl::LogAndApply(BlobVersionEdit* edit) {
  rwlock_.AssertWLockHeld();
  Status s;
  edit->SetNextFile(CurrentFileNumber() + 1);

  // Create a new writable vlog before making the durable manifest refer to
  // it. Pin it until the edit has been durably applied in memory: a failed
  // manifest sync has an indeterminate outcome and cleanup must not remove a
  // file which the on-disk manifest may already name.
  AppendableRandomAccessFile* new_file = nullptr;
  std::string new_file_name;
  if (edit->has_new_rw_file_) {
    new_file_name = VLogFileName(dbname_, edit->new_rw_file_);
    s = env_->NewAppendableRandomAccessFile(new_file_name, &new_file);
    if (!s.ok()) {
      return s;
    }
    pending_outputs_.insert(edit->new_rw_file_);
    // The manifest must not name a vlog whose directory entry can disappear
    // after a power loss. AppendableRandomAccessFile::Sync() makes both the
    // empty file and, for the POSIX implementation, its parent entry durable.
    s = new_file->Sync();
    if (!s.ok()) {
      delete new_file;
      return s;
    }
  }

  // Log to manifest
  {
    std::string manifest_name;
    if (!manifest_file_) {
      // creating new manifest file
      manifest_name = VLogManifestFileName(dbname_, manifest_number_);
      s = env_->NewWritableFile(manifest_name, &manifest_file_);
      if (s.ok()) {
        manifest_log_ = new log::Writer(manifest_file_);
        Log(options_.info_log, "WriteSnapshot to %s\n", manifest_name.c_str());
        s = WriteSnapshot(manifest_log_);
      }
    }

    if (s.ok()) {
      // Keep rwlock_ held until the durable edit and its in-memory state are
      // both complete. Otherwise another writer can observe the closed old
      // rwfile_, and concurrent GC can interleave records in manifest_log_.
      std::string encoding;
      edit->EncodeTo(&encoding);
      s = manifest_log_->AddRecord(encoding);
      if (s.ok()) {
        s = manifest_file_->Sync();
      }
      if (s.ok() && !manifest_name.empty()) {
        s = SetVLogCurrentFile(env_, dbname_, manifest_number_);
      }
    }
  }

  if (!s.ok()) {
    Log(options_.info_log, "VLOG-MANIFEST write: %s\n", s.ToString().c_str());
    delete new_file;
    // Keep the empty file: a failed manifest sync has an indeterminate
    // durability outcome. If the edit reached disk it may refer to this file;
    // otherwise recovery safely treats it as an untracked vlog.
    return s;
  }

  // Apply only after all durable metadata operations succeeded.
  if (edit->has_new_rw_file_) {
    VLogRWFile* new_rwfile =
        new VLogRWFile(options_, new_file, edit->new_rw_file_);
    s = new_rwfile->status();
    if (!s.ok()) {
      new_rwfile->Finish();
      new_rwfile->Ref();
      new_rwfile->Unref();
      return s;
    }
    new_rwfile->Ref();
    rwfile_ = new_rwfile;
    pending_outputs_.erase(edit->new_rw_file_);
  }

  for (auto p : edit->deleted_files_) {
    assert(obsolete_files_.count(p.first) == 0);
    assert(ro_files_.count(p.first) != 0);
    obsolete_files_.emplace(p.first, p.second);
  }

  for (auto f : edit->new_files_) {
    if (ro_files_.count(f.number) != 0) {
      assert(f.file_size == ro_files_[f.number].file_size);
    }
    assert(f.number <= CurrentFileNumber());
    ro_files_.emplace(f.number, f);
    if (pending_outputs_.count(f.number)) {
      pending_outputs_.erase(f.number);
    }
  }

  return s;
}

Status ValueLogImpl::WriteSnapshot(log::Writer* log) {
  rwlock_.AssertWLockHeld();
  BlobVersionEdit edit;

  edit.SetNextFile(CurrentFileNumber() + 1);
  //  Log(options_.info_log, "next file number: %llu", edit.next_file_number_);

  if (rwfile_) {
    edit.NewRWFile(rwfile_->FileNumber());
  }

  for (auto p : obsolete_files_) {
    edit.DeleteFile(p.first, p.second);
  }

  for (auto p : ro_files_) {
    VLogFileMeta& f = p.second;
    edit.AddFile(f.number, f.file_size);
    //    Log(options_.info_log, "add file: %llu", f.number);
  }

  std::string encoding;
  edit.EncodeTo(&encoding);
  return log->AddRecord(encoding);
}

Status ValueLogImpl::NewBlobDB() {
  BlobVersionEdit edit;
  edit.SetNextFile(2);

  const std::string manifest = VLogManifestFileName(dbname_, 1);
  WritableFile* file;
  Status s = env_->NewWritableFile(manifest, &file);
  if (!s.ok()) {
    return s;
  }
  {
    log::Writer log(file);
    std::string record;
    edit.EncodeTo(&record);
    s = log.AddRecord(record);
    if (s.ok()) {
      s = file->Sync();
    }
    if (s.ok()) {
      s = file->Close();
    }
  }
  delete file;
  if (s.ok()) {
    // Make "CURRENT" file that points to the new manifest file.
    s = SetVLogCurrentFile(env_, dbname_, 1);
  } else {
    env_->RemoveFile(manifest);
  }
  return s;
}

Iterator* ValueLogImpl::NewVLogFileIterator(const ReadOptions& options,
                                            uint64_t number) {
  rwlock_.AssertRLockHeld();
  Iterator* iter = nullptr;
  VLogRWFile* rwfile = rwfile_;
  if (rwfile_) rwfile_->Ref();

  if (rwfile && number == rwfile_->FileNumber()) {
    iter = rwfile->NewIterator(options);
  } else {
    auto it = ro_files_.find(number);
    if (it != ro_files_.end()) {
      iter = vlog_cache_->NewIterator(options, number, it->second.file_size);
    }
  }

  if (rwfile) rwfile->Unref();
  return iter;
}

static std::string Bytes2Size(uint64_t size) {
  char buf[100];
  if (size < (1 << 10)) {
    snprintf(buf, sizeof(buf), "%llu B",
             static_cast<unsigned long long>(size));
    return {buf};
  } else if (size < (1 << 20)) {
    snprintf(buf, sizeof(buf), "%.2f KB", double(size) / 1024.0);
  } else if (size < (1 << 30)) {
    snprintf(buf, sizeof(buf), "%.2f MB", double(size) / (1024.0 * 1024.0));
  } else {
    snprintf(buf, sizeof(buf), "%.2f GB",
             double(size) / (1024.0 * 1024.0 * 1024.0));
  }
  return buf;
}

std::string ValueLogImpl::DebugString() {
  ReadLock l(&rwlock_);
  std::string s = "=========Value Log==========\n";
  if (rwfile_ != nullptr) {
    s.append(VLogFileName("", rwfile_->FileNumber()).erase(0, 1) + ":\t" +
             Bytes2Size(rwfile_->FileSize()) + " (Writable)\n");
  }
  for (auto f : ro_files_) {
    s.append(VLogFileName("", f.second.number).erase(0, 1) + ":\t" +
             Bytes2Size(f.second.file_size) + "\n");
  }
  return s;
}

Status ValueLogImpl::CheckBlobDBExistence() {
  Status s;
  if (!env_->FileExists(VLogCurrentFileName(dbname_))) {
    if (options_.create_if_missing) {
      Log(options_.info_log, "Creating BlobDB %s since it was missing.",
          dbname_.c_str());
      s = NewBlobDB();
      if (!s.ok()) {
        return s;
      }
    } else {
      return Status::InvalidArgument(
          dbname_, "does not exist (create_if_missing is false)");
    }
  } else {
    if (options_.error_if_exists) {
      return Status::InvalidArgument(dbname_,
                                     "exists (error_if_exists is true)");
    }
  }
  return s;
}

/*
 * This function should be called only after we've WriteSnapshot to the new
 * manifest file. Otherwise, the reused rwfile may be removed.
 */
void ValueLogImpl::RemoveObsoleteFiles() {
  rwlock_.AssertWLockHeld();

  std::vector<std::string> filenames;
  env_->GetChildren(dbname_, &filenames);  // Ignoring errors on purpose
  uint64_t number;
  FileType type;
  std::vector<std::string> files_to_delete;
  for (std::string& filename : filenames) {
    if (ParseFileName(filename, &number, &type)) {
      bool keep = true;
      switch (type) {
        case kVLogCurrentFile:
          keep = true;
          break;
        case kVLogManifestFile:
          keep = (number >= manifest_number_);
          break;
        case kVLogFile:
          keep = ro_files_.find(number) != ro_files_.end() ||
                 pending_outputs_.find(number) != pending_outputs_.end() ||
                 pending_publications_.find(number) !=
                     pending_publications_.end() ||
                 (rwfile_ && number == rwfile_->FileNumber());
          if (!keep) {
            /*
             * The latest RW file.
             */
            Log(options_.info_log, "Removing untracked vlog file: %s\n",
                filename.c_str());
            break;
          }
          if (rwfile_ && number == rwfile_->FileNumber()) {
            break;
          }
          /*
           * Checking whether to remove obsolete files on disk.
           */
          if (pending_publications_.count(number) == 0 &&
              obsolete_files_.find(number) != obsolete_files_.end()) {
            assert(pending_outputs_.find(number) == pending_outputs_.end());
            SequenceNumber smallest = db_->SmallestSequence();
            // A snapshot at the GC edit sequence was created after the
            // replacement handles at that sequence and cannot reference the
            // obsolete file. Only strictly older snapshots keep it alive.
            if (smallest >= obsolete_files_[number]) {
              obsolete_files_.erase(number);
              keep = false;
            }
          }
          break;
        default:
          /*
           * LSM files will be handled by DBImpl
           */
          keep = true;
          break;
      }

      if (!keep) {
        files_to_delete.push_back(std::move(filename));
        if (type == kVLogFile) {
          ro_files_.erase(number);
          vlog_cache_->Evict(number);
        }
        Log(options_.info_log, "ValueLog Delete type=%d #%lld\n",
            static_cast<int>(type), static_cast<unsigned long long>(number));
      }
    }
  }

  // While deleting all files unblock other threads. All files being deleted
  // have unique names which will not collide with newly created files and
  // are therefore safe to delete while allowing other threads to proceed.
  rwlock_.WUnlock();
  for (const std::string& filename : files_to_delete) {
    env_->RemoveFile(dbname_ + "/" + filename);
  }
  rwlock_.WLock();
}

Status ValueLogImpl::ValidateAndTruncate(uint64_t number, uint64_t* offset,
                                         uint64_t* num_entries,
                                         uint64_t* file_size) {
  Status s;
  std::string filename = VLogFileName(dbname_, number);
  AppendableRandomAccessFile* file;
  VLogReader* reader = nullptr;

  s = env_->GetFileSize(filename, file_size);
  if (!s.ok()) {
    return s;
  }
  s = env_->NewAppendableRandomAccessFile(filename, &file);
  if (!s.ok()) {
    return s;
  }
  s = VLogReader::Open(options_, file, *file_size, &reader);
  if (!s.ok()) {
    delete file;
    return s;
  }

  Log(options_.info_log, "Validating %s, file_size=%llu", filename.c_str(),
      static_cast<unsigned long long>(*file_size));

  Status validation_status;
  bool valid = reader->Validate(offset, num_entries, &validation_status);
  /*
   * it's not expensive to reopen and re-create once more when recovering a
   * database, we delete them here and re-create later for better code
   * readability.
   */
  delete reader;
  delete file;

  if (!validation_status.ok() && !validation_status.IsCorruption()) {
    return validation_status;
  }

  if (!valid) {
    /*
     * The file's valid size is smaller than its actual size, repair it by
     * truncating it to the valid size.
     */
    const uint64_t original_file_size = *file_size;
    s = env_->TruncateFile(filename, *offset);
    if (s.ok()) {
      *file_size = *offset;
    }
    Log(options_.info_log, "%s broken, truncating it from %llu to %llu",
        filename.c_str(),
        static_cast<unsigned long long>(original_file_size),
        static_cast<unsigned long long>(*offset));
  }
  return s;
}

}  // namespace leveldb
