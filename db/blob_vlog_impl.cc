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
      manual_gc_(false),
      gc_last_run_(std::chrono::system_clock::now()) {}

Status ValueLogImpl::Put(const WriteOptions& options, const Slice& key,
                         const Slice& value, ValueHandle* handle) {
  Status s;
  ValueBatch batch;
  batch.Put(key, value);
  s = Write(options, &batch);
  *handle = batch.handles_[0];
  return s;
}

Status ValueLogImpl::Write(const WriteOptions& options, ValueBatch* batch) {
  Status s;
  batch->Finalize(rwfile_->FileNumber(), rwfile_->Offset());
  rwfile_->Write(batch);
  if (options.sync) {
    rwfile_->Sync();
  }
  if (rwfile_->FileSize() > options_.blob_max_file_size) {
    WriteLock l(&rwlock_);
    s = FinishBuild();
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
  Iterator* iter;

  if (!rwfile || handle.table_ != rwfile->FileNumber()) {
    /*
     * Get from read only files
     */
    auto it = ro_files_.find(handle.table_);
    if (it != ro_files_.end()) {
      if (handle.offset_ >= it->second.file_size) {
        return Status::InvalidArgument("value log offset is out of limit");
      }
      iter = vlog_cache_->NewIterator(options, handle.table_,
                                      it->second.file_size);
    } else {
      return Status::NotFound("value log number not exists");
    }
  } else {
    /*
     * Get from rwfile
     */
    if (handle.offset_ >= rwfile->Offset()) {
      Log(options_.info_log, "value log offset %u is out of limit %u",
          handle.offset_, rwfile->Offset());
      return Status::InvalidArgument("value log offset is out of limit");
    }
    iter = rwfile->NewIterator(options);
  }

  {
    // unlock when reading from disk
    rwlock_.RUnlock();
    std::string handle_encoding;
    handle.EncodeTo(&handle_encoding);
    iter->Seek(handle_encoding);  // expensive
    if (iter->Valid()) {
      const Slice v = iter->value();
      value->assign(v.data(), v.size());
    } else {
      Status s = Status::NotFound("value not found", iter->status().ToString());
      delete iter;
      rwlock_.RLock();
      return s;
    }
    delete iter;
    rwlock_.RLock();
  }

  return Status::OK();
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
  env_->CreateDir(dbname_);
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
    SequentialFile* file;
    s = env_->NewSequentialFile(manifest_name, &file);
    LogReporter reporter;
    reporter.status = &s;
    log::Reader reader(file, &reporter, true, 0);

    Slice record;
    std::string scratch;
    while (reader.ReadRecord(&record, &scratch) && s.ok()) {
      BlobVersionEdit edit;
      s = edit.DecodeFrom(record);
      if (edit.has_next_file_number_) {
        vlog_file_number_ = edit.next_file_number_ - 1;
      }

      if (edit.has_new_rw_file_) {
        assert(edit.new_rw_file_ <= vlog_file_number_);
        latest_rw_file = edit.new_rw_file_;
        has_latest_rw_file = true;
      }

      for (auto f : edit.new_files_) {
        assert(ro_files_.count(f.number) == 0);
        assert(f.number <= CurrentFileNumber());
        ro_files_.emplace(f.number, f);
      }

      for (auto p : edit.deleted_files_) {
        assert(obsolete_files_.count(p.first) == 0);
        assert(ro_files_.count(p.first) != 0);
        obsolete_files_.emplace(p.first, p.second);
      }
    }

    assert(s.ok());
    delete file;
    file = nullptr;

  } else {
    Log(options_.info_log,
        "VLOG MANIFEST file doesn't exist. This indicates a brand new "
        "ValueLog");
  }

  // scan over all the .vlog files
  env_->GetChildren(dbname_, &fnames);
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
        if (s.ok()) {
          Log(options_.info_log, "Adding untracked vlog file %s to ro_files",
              filename.c_str());

          VLogFileMeta f;
          f.number = number;
          f.file_size = offset;

          ro_files_.emplace(number, f);
          found_files.emplace(number);
          MarkFileNumberUsed(number);
        }
        continue;
      }

      found_files.emplace(number);

      /*
       * check file size
       */
      uint64_t file_size;
      s = env_->GetFileSize(DBFilePath(dbname_, filename), &file_size);
      assert(s.ok());
      Log(options_.info_log, "filename %s: %s", filename.c_str(),
          (number == latest_rw_file || file_size == ro_files_[number].file_size)
              ? "ok"
              : "no");
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
    ValidateAndTruncate(latest_rw_file, &offset, &num_entries, &file_size);

    if (offset <= (options_.blob_max_file_size / 2)) {
      /*
       * reuse the .vlog file
       */
      Log(options_.info_log, "Reusing %s, ValidSize/FileSize = %llu/%llu",
          filename.c_str(), offset, file_size);
      reuse = true;
    }
#ifndef NDEBUG
    assert(s.ok());
    env_->GetFileSize(filename, &file_size);
    assert(offset == file_size);
#endif
  }

  if (!reuse) {
    offset = 0;
    num_entries = 0;
    BlobVersionEdit edit;
    if (has_latest_rw_file) {
      edit.AddFile(latest_rw_file, file_size);
    }
    edit.NewRWFile(NewFileNumber());
    LogAndApply(&edit);
  } else {
    s = env_->NewAppendableRandomAccessFile(
        VLogFileName(dbname_, latest_rw_file), &file);
    assert(s.ok());
    rwfile_ = new VLogRWFile(options_, file, latest_rw_file, true, offset,
                             num_entries);
    rwfile_->Ref();

    /*
     * We log an empty BlobVersionEdit to create the new manifest file, and
     * WriteSnapshot to the new manifest, to avoid losing the snapshot from the
     * last manifest file.
     */
    BlobVersionEdit edit;
    s = LogAndApply(&edit);
    assert(s.ok());
  }

  RemoveObsoleteFiles();
  return s;
}

ValueLogImpl::~ValueLogImpl() {
  rwfile_->Finish();
  rwfile_->Unref();
  rwfile_ = nullptr;

  mutex_.Lock();
  shutdown_.store(true, std::memory_order_release);
  while (bg_garbage_collection_) {
    bg_work_cv_.Wait();
  }
  mutex_.Unlock();

  delete manifest_log_;
  delete manifest_file_;
  delete vlog_cache_;
}

Status ValueLogImpl::FinishBuild() {
  rwlock_.AssertWLockHeld();
  BlobVersionEdit edit;
  VLogFileMeta f;

  rwfile_->Finish();

  mutex_.Lock();
  MaybeScheduleGC();
  mutex_.Unlock();

  /*
   * We add rwfile to ro_files in advance
   */
  f.number = rwfile_->FileNumber();
  f.file_size = rwfile_->FileSize();
  ro_files_.emplace(f.number, f);

  edit.AddFile(rwfile_->FileNumber(), rwfile_->FileSize());
  edit.NewRWFile(NewFileNumber());

  rwfile_->Unref();
  rwfile_ = nullptr;

  return LogAndApply(&edit);
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

    rwlock_.WUnlock();

    // add new records
    std::string encoding;
    edit->EncodeTo(&encoding);
    s = manifest_log_->AddRecord(encoding);
    if (s.ok()) {
      s = manifest_file_->Sync();
    }
    if (!s.ok()) {
      Log(options_.info_log, "VLOG-MANIFEST write: %s\n", s.ToString().c_str());
    }

    if (s.ok() && !manifest_name.empty()) {
      s = SetVLogCurrentFile(env_, dbname_, manifest_number_);
    }

    rwlock_.WLock();
  }

  // Apply
  if (edit->has_new_rw_file_) {
    AppendableRandomAccessFile* file;
    s = env_->NewAppendableRandomAccessFile(
        VLogFileName(dbname_, edit->new_rw_file_), &file);
    rwfile_ = new VLogRWFile(options_, file, edit->new_rw_file_);
    rwfile_->Ref();
  }

  if (!s.ok()) {
    return s;
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
    snprintf(buf, sizeof(buf), "%llu B", size);
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
  s.append(VLogFileName("", rwfile_->FileNumber()).erase(0, 1) + ":\t" +
           Bytes2Size(rwfile_->FileSize()) + " (Writable)\n");
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
          if (obsolete_files_.find(number) != obsolete_files_.end()) {
            assert(pending_outputs_.find(number) == pending_outputs_.end());
            SequenceNumber smallest = db_->SmallestSequence();
            if (smallest > obsolete_files_[number]) {
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
  VLogReader* reader;

  s = env_->GetFileSize(filename, file_size);
  s = env_->NewAppendableRandomAccessFile(filename, &file);
  assert(s.ok());
  s = VLogReader::Open(options_, file, *file_size, &reader);
  if (!s.ok()) {
    delete file;
    return s;
  }

  Log(options_.info_log, "Validating %s, file_size=%llu", filename.c_str(),
      *file_size);

  bool valid = reader->Validate(offset, num_entries);
  /*
   * it's not expensive to reopen and re-create once more when recovering a
   * database, we delete them here and re-create later for better code
   * readability.
   */
  delete reader;
  delete file;

  if (!valid) {
    /*
     * The file's valid size is smaller than its actual size, repair it by
     * truncating it to the valid size.
     */
    s = env_->TruncateFile(filename, *offset);
    Log(options_.info_log, "%s broken, truncating it from %llu to %llu",
        filename.c_str(), *file_size, *offset);
  }
  return s;
}

}  // namespace leveldb