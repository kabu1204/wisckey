// Copyright (c) 2023. Chengye YU <yuchengye2013 AT outlook.com>
// SPDX-License-Identifier: BSD-3-Clause

#include "db/blob_db.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <deque>
#include <limits>
#include <map>
#include <sstream>

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
                DividedEntry* inline_entry, bool* has_inline_entry,
                std::vector<DividedEntry>* additional_entries)
      : threshold_(threshold),
        large_(large),
        inline_entry_(inline_entry),
        has_inline_entry_(has_inline_entry),
        additional_entries_(additional_entries) {}
  ~DivideHandler() override = default;

  void Put(const Slice& key, const Slice& value) override {
    if (value.size() >= threshold_) {
      const size_t handle_index = large_->NumEntries();
      large_->PutBorrowed(key, value);
      Add({DividedEntryType::kValueHandle, key, Slice(), handle_index});
    } else {
      Add({DividedEntryType::kValue, key, value, 0});
    }
  }

  void Delete(const Slice& key) override {
    Add({DividedEntryType::kDeletion, key, Slice(), 0});
  }

 private:
  void Add(const DividedEntry& entry) {
    if (!*has_inline_entry_) {
      *inline_entry_ = entry;
      *has_inline_entry_ = true;
    } else {
      additional_entries_->push_back(entry);
    }
  }

  const size_t threshold_;
  ValueBatch* const large_;
  DividedEntry* const inline_entry_;
  bool* const has_inline_entry_;
  std::vector<DividedEntry>* const additional_entries_;
};

class BlobDB::WriteCoordinator {
 public:
  explicit WriteCoordinator(BlobDB* owner) : owner_(owner) {}

  ~WriteCoordinator() {
    MutexLock lock(&mutex_);
    assert(queue_.empty());
    assert(!publisher_active_);
    for (const auto& target : dirty_files_) target.first->Unref();
  }

  Status Write(const WriteOptions& options, WriteBatch* updates,
               WriteCallback* callback) {
    if (updates == nullptr) {
      return Status::InvalidArgument("null BlobDB write batch");
    }
    Request request(&mutex_);
    request.options = options;
    request.callback = callback;
    const size_t update_count = WriteBatchInternal::Count(updates);
    if (update_count > 1) request.additional_entries.reserve(update_count - 1);
    DivideHandler handler(owner_->options_.blob_value_size_threshold,
                          &request.large, &request.inline_entry,
                          &request.has_inline_entry,
                          &request.additional_entries);
    Status s = updates->Iterate(&handler);
    if (!s.ok()) return s;

    bool run_publisher = false;
    bool needs_vlog_write = false;
    {
      MutexLock admission(&admission_mutex_);
      if (failed_.load(std::memory_order_acquire)) {
        mutex_.Lock();
        s = failure_status_;
        mutex_.Unlock();
        return s;
      }

      // Assign the publication ticket before reserving vlog space. A
      // reservation/rollover failure then becomes an ordered failed request:
      // already-admitted prefixes may finish, while this and every later
      // request are prevented from reaching the WAL.
      request.ticket = next_ticket_++;
      if (request.large.NumEntries() != 0) {
        s = owner_->vlog_->ReserveWrite(&request.large, true,
                                        &request.reservation);
        if (s.ok()) {
          request.state = RequestState::kWriting;
          needs_vlog_write = true;
        } else {
          request.preparation_status = s;
          request.state = RequestState::kFailed;
        }
      } else {
        request.batch = updates;
        request.state = RequestState::kReady;
      }
      mutex_.Lock();
      queue_.push_back(&request);
      ++requests_admitted_;
      if (!s.ok()) RecordFailure(request.ticket, s);
      if (needs_vlog_write) {
        ++vlog_inflight_;
        max_vlog_inflight_ = std::max(max_vlog_inflight_, vlog_inflight_);
      }
      max_queue_depth_ = std::max<uint64_t>(max_queue_depth_, queue_.size());
      run_publisher = MaybeBecomePublisher();
      mutex_.Unlock();
    }

    if (needs_vlog_write) {
      // Handle encoding and transformed-batch construction do not affect
      // reservation order. Keep this allocation/copy work out of the short
      // admission critical section together with the positioned vlog I/O.
      BuildTransformed(&request);
      request.batch = &request.transformed;
      const Status write_status =
          owner_->vlog_->WriteReserved(request.reservation, &request.large);
      mutex_.Lock();
      assert(vlog_inflight_ > 0);
      --vlog_inflight_;
      request.preparation_status = write_status;
      request.state = write_status.ok() ? RequestState::kReady
                                        : RequestState::kFailed;
      if (!write_status.ok()) RecordFailure(request.ticket, write_status);
      run_publisher = MaybeBecomePublisher() || run_publisher;
      mutex_.Unlock();
    }

    if (run_publisher) PublishReady();

    WaitForCompletion(&request);
    // The publisher stores status before the release-store to completed.
    // WaitForCompletion observes that store with acquire semantics, so taking
    // the coordinator mutex again here only creates a completion convoy.
    return request.status;
  }

  Status Sync() {
    Request request(&mutex_);
    request.options.sync = true;
    request.barrier = true;
    request.state = RequestState::kReady;

    bool run_publisher;
    {
      MutexLock admission(&admission_mutex_);
      if (failed_.load(std::memory_order_acquire)) {
        mutex_.Lock();
        Status s = failure_status_;
        mutex_.Unlock();
        return s;
      }
      request.ticket = next_ticket_++;
      mutex_.Lock();
      queue_.push_back(&request);
      ++requests_admitted_;
      max_queue_depth_ = std::max<uint64_t>(max_queue_depth_, queue_.size());
      run_publisher = MaybeBecomePublisher();
      mutex_.Unlock();
    }
    if (run_publisher) PublishReady();

    WaitForCompletion(&request);
    return request.status;
  }

  Status WriteLSM(const WriteOptions& options, WriteBatch* updates,
                  WriteCallback* callback) {
    if (updates == nullptr) {
      return Status::InvalidArgument("null internal LSM write batch");
    }
    Request request(&mutex_);
    request.options = options;
    request.callback = callback;
    request.batch = updates;
    request.state = RequestState::kReady;

    bool run_publisher;
    {
      MutexLock admission(&admission_mutex_);
      if (failed_.load(std::memory_order_acquire)) {
        mutex_.Lock();
        Status s = failure_status_;
        mutex_.Unlock();
        return s;
      }
      request.ticket = next_ticket_++;
      mutex_.Lock();
      queue_.push_back(&request);
      ++requests_admitted_;
      max_queue_depth_ = std::max<uint64_t>(max_queue_depth_, queue_.size());
      run_publisher = MaybeBecomePublisher();
      mutex_.Unlock();
    }
    if (run_publisher) PublishReady();

    WaitForCompletion(&request);
    return request.status;
  }

  std::string DebugString() {
    MutexLock lock(&mutex_);
    std::ostringstream out;
    out << "requests=" << requests_admitted_
        << " groups=" << publication_groups_
        << " grouped_requests=" << grouped_requests_
        << " max_queue_depth=" << max_queue_depth_
        << " max_vlog_inflight=" << max_vlog_inflight_
        << " vlog_syncs=" << vlog_syncs_.load(std::memory_order_relaxed);
    return out.str();
  }

 private:
  enum class RequestState { kWriting, kReady, kFailed, kPublishing, kDone };

  struct Request {
    explicit Request(port::Mutex* mutex) : cv(mutex) {}

    uint64_t ticket = 0;
    WriteOptions options;
    WriteCallback* callback = nullptr;
    bool barrier = false;
    RequestState state = RequestState::kReady;
    Status preparation_status;
    Status status;
    DBImpl::WriteState write_state = DBImpl::WriteState::kNotWritten;
    std::atomic<bool> completed{false};
    std::atomic<bool> publish_requested{false};
    ValueBatch large;
    DividedEntry inline_entry;
    bool has_inline_entry = false;
    std::vector<DividedEntry> additional_entries;
    WriteBatch transformed;
    WriteBatch* batch = nullptr;
    ValueLogImpl::WriteReservation reservation;
    port::CondVar cv;
  };

  static void BuildTransformed(Request* request) {
    std::string handle_encoding;
    const auto append_entry = [&](const DividedEntry& entry) {
      switch (entry.type) {
        case DividedEntryType::kValue:
          request->transformed.Put(entry.key, entry.value);
          break;
        case DividedEntryType::kValueHandle:
          assert(entry.handle_index < request->large.NumEntries());
          handle_encoding.clear();
          request->large.Handle(entry.handle_index).EncodeTo(&handle_encoding);
          WriteBatchInternal::Put(&request->transformed, entry.key,
                                  handle_encoding, kTypeValueHandle);
          break;
        case DividedEntryType::kDeletion:
          request->transformed.Delete(entry.key);
          break;
      }
    };
    if (request->has_inline_entry) {
      append_entry(request->inline_entry);
    }
    for (const DividedEntry& entry : request->additional_entries) {
      append_entry(entry);
    }
  }

  bool MaybeBecomePublisher() {
    mutex_.AssertHeld();
    if (publisher_active_ || queue_.empty() ||
        queue_.front()->state == RequestState::kWriting) {
      return false;
    }
    publisher_active_ = true;
    return true;
  }

  void WaitForCompletion(Request* request) {
    // Most grouped publications complete within a few microseconds. A short
    // userspace wait avoids a futex sleep/wakeup round trip and lets followers
    // re-enter admission quickly enough to keep the next WAL group populated.
    const auto deadline = std::chrono::steady_clock::now() +
                          std::chrono::microseconds(20);
    while (!request->completed.load(std::memory_order_acquire) &&
           std::chrono::steady_clock::now() < deadline) {
      if (request->publish_requested.exchange(false,
                                              std::memory_order_acq_rel)) {
        PublishReady();
      }
    }
    if (request->completed.load(std::memory_order_acquire)) return;

    mutex_.Lock();
    while (!request->completed.load(std::memory_order_acquire)) {
      if (request->publish_requested.exchange(false,
                                              std::memory_order_acq_rel)) {
        mutex_.Unlock();
        PublishReady();
        mutex_.Lock();
        continue;
      }
      request->cv.Wait();
    }
    mutex_.Unlock();
  }

  void RecordFailure(uint64_t ticket, const Status& status) {
    mutex_.AssertHeld();
    if (status.ok()) return;
    if (ticket < failed_ticket_) {
      failed_ticket_ = ticket;
      failure_status_ = status;
      failed_.store(true, std::memory_order_release);
    }
  }

  void ReleaseGroupReservations(
      const std::array<Request*, 64>& group, size_t group_size,
      bool release_publication) {
    if (release_publication) {
      uint64_t file_number = 0;
      size_t file_count = 0;
      for (size_t i = 0; i < group_size; ++i) {
        auto& reservation = group[i]->reservation;
        if (!reservation.publication_pinned) continue;
        if (file_count != 0 && reservation.file_number != file_number) {
          owner_->vlog_->ReleasePublication(file_number, file_count);
          file_count = 0;
        }
        file_number = reservation.file_number;
        ++file_count;
        reservation.publication_pinned = false;
      }
      if (file_count != 0) {
        owner_->vlog_->ReleasePublication(file_number, file_count);
      }
    }

    for (size_t i = 0; i < group_size; ++i) {
      owner_->vlog_->ReleaseReservation(&group[i]->reservation, false);
    }
  }

  void AddDirtyTarget(const ValueLogImpl::WriteReservation& reservation) {
    if (reservation.file == nullptr) return;
    auto inserted = dirty_files_.emplace(reservation.file, reservation.end);
    if (inserted.second) {
      reservation.file->Ref();
    } else {
      inserted.first->second =
          std::max(inserted.first->second, reservation.end);
    }
  }

  Status SyncDirtyTargets() {
    Status status;
    for (const auto& target : dirty_files_) {
      status = target.first->SyncThrough(target.second);
      if (!status.ok()) break;
      vlog_syncs_.fetch_add(1, std::memory_order_relaxed);
    }
    for (const auto& target : dirty_files_) target.first->Unref();
    dirty_files_.clear();
    return status;
  }

  void CompleteWithoutPublication(Request* request, const Status& status) {
    owner_->vlog_->ReleaseReservation(&request->reservation, true);
    mutex_.Lock();
    assert(!queue_.empty() && queue_.front() == request);
    queue_.pop_front();
    request->status = status;
    request->write_state = DBImpl::WriteState::kNotWritten;
    request->state = RequestState::kDone;
    request->completed.store(true, std::memory_order_release);
    request->cv.Signal();
    mutex_.Unlock();
  }

  void PublishReady() {
    constexpr size_t kMaxGroupRequests = 64;
    constexpr size_t kMaxGroupBytes = 1 << 20;

    while (true) {
      std::array<Request*, kMaxGroupRequests> group;
      size_t group_size = 0;
      bool group_sync = false;

      mutex_.Lock();
      if (queue_.empty() ||
          queue_.front()->state == RequestState::kWriting) {
        publisher_active_ = false;
        mutex_.Unlock();
        return;
      }

      Request* front = queue_.front();
      if (front->ticket >= failed_ticket_ ||
          front->state == RequestState::kFailed) {
        const Status status = failure_status_.ok()
                                  ? front->preparation_status
                                  : failure_status_;
        mutex_.Unlock();
        CompleteWithoutPublication(front, status);
        continue;
      }

      size_t group_bytes = 0;
      bool saw_sync = false;
      for (Request* request : queue_) {
        if (request->state != RequestState::kReady ||
            request->ticket >= failed_ticket_) {
          break;
        }
        if (request->callback != nullptr) {
          if (group_size != 0) break;
          group[group_size++] = request;
          group_sync = request->options.sync;
          break;
        }
        if (saw_sync && !request->options.sync && !request->barrier) break;
        const size_t request_bytes =
            request->batch == nullptr
                ? 0
                : WriteBatchInternal::ByteSize(request->batch);
        if (group_size != 0 &&
            (group_size == kMaxGroupRequests ||
             request_bytes >
                 kMaxGroupBytes - std::min(group_bytes, kMaxGroupBytes))) {
          break;
        }
        group[group_size++] = request;
        group_bytes += request_bytes;
        group_sync = group_sync || request->options.sync || request->barrier;
        saw_sync = saw_sync || request->options.sync || request->barrier;
        if (request->barrier) break;
      }
      assert(group_size != 0);
      for (size_t i = 0; i < group_size; ++i) {
        group[i]->state = RequestState::kPublishing;
      }
      mutex_.Unlock();

      WriteBatch* publish_batch = nullptr;
      WriteCallback* callback = nullptr;
      if (group_size == 1) {
        publish_batch = group[0]->batch;
        callback = group[0]->callback;
      } else {
        aggregate_.Clear();
        for (size_t i = 0; i < group_size; ++i) {
          Request* request = group[i];
          if (request->batch != nullptr) {
            WriteBatchInternal::Append(&aggregate_, request->batch);
          }
        }
        if (WriteBatchInternal::Count(&aggregate_) != 0) {
          publish_batch = &aggregate_;
        }
      }

      for (size_t i = 0; i < group_size; ++i) {
        AddDirtyTarget(group[i]->reservation);
      }

      Status status;
      DBImpl::WriteState write_state = DBImpl::WriteState::kNotWritten;
      if (group_sync) status = SyncDirtyTargets();
      if (status.ok()) {
        if (publish_batch == nullptr) {
          status = owner_->db_->Sync();
          if (status.ok()) write_state = DBImpl::WriteState::kApplied;
        } else {
          WriteOptions write_options;
          write_options.sync = group_sync;
          status = owner_->db_->WriteOrderedWithOutcome(
              write_options, publish_batch, callback, &write_state);
        }
      }

      ReleaseGroupReservations(
          group, group_size,
          write_state != DBImpl::WriteState::kMayBeWritten);

      mutex_.Lock();
      // Callback rejection is request-local and happens before WAL emission.
      // Every other publication error is terminal: a sync failure makes
      // durability indeterminate, and a WAL/memtable failure must not allow a
      // later ticket to become visible ahead of the failed one.
      if (!status.ok() &&
          write_state != DBImpl::WriteState::kCallbackRejected) {
        RecordFailure(group[0]->ticket, status);
      }
      ++publication_groups_;
      grouped_requests_ += group_size;
      for (size_t i = 0; i < group_size; ++i) {
        Request* request = group[i];
        assert(!queue_.empty() && queue_.front() == request);
        queue_.pop_front();
        request->status = status;
        request->write_state = write_state;
        request->state = RequestState::kDone;
        request->completed.store(true, std::memory_order_release);
        request->cv.Signal();
      }

      // Hand the serialized publication token to the next ready caller. The
      // current publisher can return and start another positioned write while
      // that caller publishes, avoiding a single foreground thread draining
      // several small groups in succession.
      if (queue_.empty() ||
          queue_.front()->state == RequestState::kWriting) {
        publisher_active_ = false;
      } else {
        queue_.front()->publish_requested.store(true,
                                                std::memory_order_release);
        queue_.front()->cv.Signal();
      }
      mutex_.Unlock();
      return;
    }
  }

  BlobDB* const owner_;
  port::Mutex admission_mutex_;
  port::Mutex mutex_;
  std::deque<Request*> queue_ GUARDED_BY(mutex_);
  bool publisher_active_ GUARDED_BY(mutex_) = false;
  uint64_t next_ticket_ GUARDED_BY(admission_mutex_) = 1;
  uint64_t failed_ticket_ GUARDED_BY(mutex_) =
      std::numeric_limits<uint64_t>::max();
  std::atomic<bool> failed_{false};
  Status failure_status_ GUARDED_BY(mutex_);
  // Only the foreground publisher accesses this batch. Reuse its allocation
  // across WAL groups instead of allocating once per grouped publication.
  WriteBatch aggregate_;
  std::map<VLogRWFile*, uint32_t> dirty_files_;
  uint64_t requests_admitted_ GUARDED_BY(mutex_) = 0;
  uint64_t publication_groups_ GUARDED_BY(mutex_) = 0;
  uint64_t grouped_requests_ GUARDED_BY(mutex_) = 0;
  uint64_t max_queue_depth_ GUARDED_BY(mutex_) = 0;
  uint64_t vlog_inflight_ GUARDED_BY(mutex_) = 0;
  uint64_t max_vlog_inflight_ GUARDED_BY(mutex_) = 0;
  std::atomic<uint64_t> vlog_syncs_{0};
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
  *vlog = vlog_impl;
  return s;
}

BlobDB::BlobDB(const Options& options, std::string dbname, DB* db,
               ValueLogImpl* vlog)
    : options_(options),
      dbname_(std::move(dbname)),
      db_(reinterpret_cast<DBImpl*>(db)),
      vlog_(vlog),
      write_coordinator_(new WriteCoordinator(this)) {}

BlobDB::~BlobDB() {
  vlog_->StopBackgroundWork();
  delete write_coordinator_;
  delete vlog_;
  delete db_;
}

Status BlobDB::Write(const WriteOptions& options, WriteBatch* updates) {
  return Write(options, updates, nullptr);
}

Status BlobDB::Write(const WriteOptions& options, WriteBatch* updates,
                     WriteCallback* callback) {
  return write_coordinator_->Write(options, updates, callback);
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
  result += "\nBlob write pipeline: ";
  result += write_coordinator_->DebugString();
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
  if (property == "leveldb.blobdb.write-pipeline") {
    *value = write_coordinator_->DebugString();
    return true;
  }
  bool ret = db_->GetProperty(property, value);
  if (ret && property == "leveldb.stats") {
    value->append("\n");
    value->append(vlog_->DebugString());
    value->append("\nBlob write pipeline: ");
    value->append(write_coordinator_->DebugString());
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
  if (options.blob_max_background_gc < 1 ||
      options.blob_max_background_gc > 2) {
    return Status::InvalidArgument(
        "blob_max_background_gc must be either 1 or 2");
  }
  Log(options.info_log,
      "Creating BlobDB, "
      "ValueThreshold = %lu\n\t"
      "BlobMaxFileSize = %lu\n\t"
      "BlobGCSizeDiscardThreshold = %d\n\t"
      "BlobGCNumDiscardThreshold = %d\n\t"
      "BlobGCInterval = %d\n\t"
      "BlobMaxBackgroundGC = %d\n\t"
      "BlobBackgroundReadThreads = %d",
      options.blob_value_size_threshold, options.blob_max_file_size,
      options.blob_gc_size_discard_threshold,
      options.blob_gc_num_discard_threshold, options.blob_gc_interval,
      options.blob_max_background_gc,
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
  const int desired_background_threads = options.blob_background_read_threads +
                                         1 +  // LSM compaction
                                         options.blob_max_background_gc;
  options.env->SetPoolBackgroundThreads(std::max(
      options.env->GetPoolBackgroundThreads(), desired_background_threads));
  BlobDB* dbWrapper = new BlobDB(options, name, db, vlog);
  vlog->owner_ = dbWrapper;
  vlog->StartGCThread();
  *dbptr = dbWrapper;
  return s;
}

void BlobDB::ManualGC(uint64_t number) { vlog_->ManualGC(number); }

Status BlobDB::RunVLogGC() { return vlog_->RunGC(); }

Status BlobDB::VLogBGError() {
  MutexLock l(&vlog_->mutex_);
  return vlog_->bg_error_;
}

Status BlobDB::SyncLSM() { return write_coordinator_->Sync(); }

Status BlobDB::WriteLSM(const WriteOptions& options, WriteBatch* updates,
                        WriteCallback* callback) {
  return write_coordinator_->WriteLSM(options, updates, callback);
}

void BlobDB::RemoveObsoleteBlob() {
  WriteLock l(&vlog_->rwlock_);
  vlog_->RemoveObsoleteFiles();
}

void BlobDB::WaitVLogGC() {
  MutexLock l(&vlog_->mutex_);
  while (vlog_->bg_gc_jobs_ != 0 || !vlog_->manual_gc_files_.empty()) {
    vlog_->bg_work_cv_.Wait();
  }
}

}  // namespace leveldb
