// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
// Modifications Copyright 2023 Chengye YU <yuchengye2013 AT outlook.com>.

#include "db/db_iter.h"

#include "db/blob_vlog_impl.h"
#include "db/db_impl.h"
#include "db/dbformat.h"
#include "db/filename.h"
#include <deque>
#include <memory>

#include "leveldb/env.h"
#include "leveldb/iterator.h"

#include "port/port.h"
#include "util/logging.h"
#include "util/mutexlock.h"
#include "util/random.h"

namespace leveldb {

#if 0
static void DumpInternalIter(Iterator* iter) {
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    ParsedInternalKey k;
    if (!ParseInternalKey(iter->key(), &k)) {
      std::fprintf(stderr, "Corrupt '%s'\n", EscapeString(iter->key()).c_str());
    } else {
      std::fprintf(stderr, "@ '%s'\n", k.DebugString().c_str());
    }
  }
}
#endif

namespace {

// Memtables and sstables that make the DB representation contain
// (userkey,seq,type) => uservalue entries.  DBIter
// combines multiple entries for the same userkey found in the DB
// representation into a single entry while accounting for sequence
// numbers, deletion markers, overwrites, etc.
class DBIter : public Iterator {
 public:
  // Which direction is the iterator currently moving?
  // (1) When moving forward, the internal iterator is positioned at
  //     the exact entry that yields this->key(), this->value()
  // (2) When moving backwards, the internal iterator is positioned
  //     just before all entries whose user key == this->key().
  enum Direction { kForward, kReverse };

  DBIter(DBImpl* db, const Comparator* cmp, Iterator* iter, SequenceNumber s,
         uint32_t seed)
      : db_(db),
        user_comparator_(cmp),
        iter_(iter),
        sequence_(s),
        direction_(kForward),
        valid_(false),
        rnd_(seed),
        bytes_until_read_sampling_(RandomCompactionPeriod()) {}

  DBIter(const DBIter&) = delete;
  DBIter& operator=(const DBIter&) = delete;

  ~DBIter() override { delete iter_; }
  bool Valid() const override { return valid_; }
  Slice key() const override {
    assert(valid_);
    return (direction_ == kForward) ? ExtractUserKey(iter_->key()) : saved_key_;
  }
  Slice value() const override {
    assert(valid_);
    return (direction_ == kForward) ? iter_->value() : saved_value_;
  }
  Status status() const override {
    if (status_.ok()) {
      return iter_->status();
    } else {
      return status_;
    }
  }

  void Next() override;
  void Prev() override;
  void Seek(const Slice& target) override;
  void SeekToFirst() override;
  void SeekToLast() override;

  ValueType valueType() const {
    assert(valid_);
    return valueType_;
  }

 private:
  void FindNextUserEntry(bool skipping, std::string* skip);
  void FindPrevUserEntry();
  bool ParseKey(ParsedInternalKey* key);

  inline void SaveKey(const Slice& k, std::string* dst) {
    dst->assign(k.data(), k.size());
  }

  inline void ClearSavedValue() {
    if (saved_value_.capacity() > 1048576) {
      std::string empty;
      swap(empty, saved_value_);
    } else {
      saved_value_.clear();
    }
  }

  // Picks the number of bytes that can be read until a compaction is scheduled.
  size_t RandomCompactionPeriod() {
    return rnd_.Uniform(2 * config::kReadBytesPeriod);
  }

  DBImpl* db_;
  ValueType valueType_;  // current key's ValueType
  const Comparator* const user_comparator_;
  Iterator* const iter_;
  SequenceNumber const sequence_;
  Status status_;
  std::string saved_key_;    // == current key when direction_==kReverse
  std::string saved_value_;  // == current raw value when direction_==kReverse
  Direction direction_;
  bool valid_;
  Random rnd_;
  size_t bytes_until_read_sampling_;
};

inline bool DBIter::ParseKey(ParsedInternalKey* ikey) {
  Slice k = iter_->key();

  size_t bytes_read = k.size() + iter_->value().size();
  while (bytes_until_read_sampling_ < bytes_read) {
    bytes_until_read_sampling_ += RandomCompactionPeriod();
    db_->RecordReadSample(k);
  }
  assert(bytes_until_read_sampling_ >= bytes_read);
  bytes_until_read_sampling_ -= bytes_read;

  if (!ParseInternalKey(k, ikey)) {
    status_ = Status::Corruption("corrupted internal key in DBIter");
    return false;
  } else {
    return true;
  }
}

void DBIter::Next() {
  assert(valid_);

  if (direction_ == kReverse) {  // Switch directions?
    direction_ = kForward;
    // iter_ is pointing just before the entries for this->key(),
    // so advance into the range of entries for this->key() and then
    // use the normal skipping code below.
    if (!iter_->Valid()) {
      iter_->SeekToFirst();
    } else {
      iter_->Next();
    }
    if (!iter_->Valid()) {
      valid_ = false;
      saved_key_.clear();
      return;
    }
    // saved_key_ already contains the key to skip past.
  } else {
    // Store in saved_key_ the current key so we skip it below.
    SaveKey(ExtractUserKey(iter_->key()), &saved_key_);

    // iter_ is pointing to current key. We can now safely move to the next to
    // avoid checking current key.
    iter_->Next();
    if (!iter_->Valid()) {
      valid_ = false;
      saved_key_.clear();
      return;
    }
  }

  FindNextUserEntry(true, &saved_key_);
}

void DBIter::FindNextUserEntry(bool skipping, std::string* skip) {
  // Loop until we hit an acceptable entry to yield
  assert(iter_->Valid());
  assert(direction_ == kForward);
  do {
    ParsedInternalKey ikey;
    if (ParseKey(&ikey) && ikey.sequence <= sequence_) {
      switch (ikey.type) {
        case kTypeDeletion:
          // Arrange to skip all upcoming entries for this key since
          // they are hidden by this deletion.
          SaveKey(ikey.user_key, skip);
          skipping = true;
          break;
        case kTypeValueHandle:
        case kTypeValue:
          if (skipping &&
              user_comparator_->Compare(ikey.user_key, *skip) <= 0) {
            // Entry hidden
          } else {
            valid_ = true;
            valueType_ = ikey.type;
            saved_key_.clear();
            return;
          }
          break;
      }
    }
    iter_->Next();
  } while (iter_->Valid());
  saved_key_.clear();
  valid_ = false;
}

void DBIter::Prev() {
  assert(valid_);

  if (direction_ == kForward) {  // Switch directions?
    // iter_ is pointing at the current entry.  Scan backwards until
    // the key changes so we can use the normal reverse scanning code.
    assert(iter_->Valid());  // Otherwise valid_ would have been false
    SaveKey(ExtractUserKey(iter_->key()), &saved_key_);
    while (true) {
      iter_->Prev();
      if (!iter_->Valid()) {
        valid_ = false;
        saved_key_.clear();
        ClearSavedValue();
        return;
      }
      if (user_comparator_->Compare(ExtractUserKey(iter_->key()), saved_key_) <
          0) {
        break;
      }
    }
    direction_ = kReverse;
  }

  FindPrevUserEntry();
}

void DBIter::FindPrevUserEntry() {
  assert(direction_ == kReverse);

  ValueType value_type = kTypeDeletion;
  if (iter_->Valid()) {
    do {
      ParsedInternalKey ikey;
      if (ParseKey(&ikey) && ikey.sequence <= sequence_) {
        if ((value_type != kTypeDeletion) &&
            user_comparator_->Compare(ikey.user_key, saved_key_) < 0) {
          // We encountered a non-deleted value in entries for previous keys,
          break;
        }
        value_type = ikey.type;
        if (value_type == kTypeDeletion) {
          saved_key_.clear();
          ClearSavedValue();
        } else {
          Slice raw_value = iter_->value();
          if (saved_value_.capacity() > raw_value.size() + 1048576) {
            std::string empty;
            swap(empty, saved_value_);
          }
          SaveKey(ExtractUserKey(iter_->key()), &saved_key_);
          saved_value_.assign(raw_value.data(), raw_value.size());
        }
      }
      iter_->Prev();
    } while (iter_->Valid());
  }

  if (value_type == kTypeDeletion) {
    // End
    valid_ = false;
    saved_key_.clear();
    ClearSavedValue();
    direction_ = kForward;
  } else {
    valueType_ = value_type;
    valid_ = true;
  }
}

void DBIter::Seek(const Slice& target) {
  direction_ = kForward;
  ClearSavedValue();
  saved_key_.clear();
  AppendInternalKey(&saved_key_,
                    ParsedInternalKey(target, sequence_, kValueTypeForSeek));
  iter_->Seek(saved_key_);
  if (iter_->Valid()) {
    FindNextUserEntry(false, &saved_key_ /* temporary storage */);
  } else {
    valid_ = false;
  }
}

void DBIter::SeekToFirst() {
  direction_ = kForward;
  ClearSavedValue();
  iter_->SeekToFirst();
  if (iter_->Valid()) {
    FindNextUserEntry(false, &saved_key_ /* temporary storage */);
  } else {
    valid_ = false;
  }
}

void DBIter::SeekToLast() {
  direction_ = kReverse;
  ClearSavedValue();
  iter_->SeekToLast();
  FindPrevUserEntry();
}

class BlobDBIter : public Iterator {
 public:
  BlobDBIter(DBIter* dbiter, ValueLogImpl* vlog, Env* env,
             bool prefetch = false, int max_prefetch = 16)
      : iter_(dbiter),
        vlog_(vlog),
        env_(env),
        current_value_valid_(false),
        isValueHandle_(false),
        prefetch_(prefetch && max_prefetch > 0),
        max_prefetch_(max_prefetch > 0
                          ? static_cast<size_t>(max_prefetch)
                          : static_cast<size_t>(0)),
        forward_(true),
        positioned_(false),
        pending_reads_(0),
        cv_(&mutex_),
        current_(nullptr) {}
  BlobDBIter(const BlobDBIter&) = delete;
  BlobDBIter& operator=(const BlobDBIter&) = delete;

  ~BlobDBIter() override {
    {
      MutexLock l(&mutex_);
      if (prefetch_) {
        CancelPrefetch();
        current_.reset();
        while (pending_reads_ != 0) {
          cv_.Wait();
        }
      }
    }
    delete iter_;
  }

  bool Valid() const override {
    if (prefetch_) {
      return current_ != nullptr && current_->valid;
    }
    return iter_->Valid() && current_value_valid_;
  }

  Slice key() const override {
    if (prefetch_) {
      assert(current_ != nullptr && current_->valid);
      return current_->key;
    }
    assert(iter_->Valid() && current_value_valid_);
    return iter_->key();
  }

  Slice value() const override {
    if (prefetch_) {
      assert(current_ != nullptr && current_->valid);
      return current_->value;
    }
    assert(iter_->Valid() && current_value_valid_);
    return isValueHandle_ ? indirect_value_ : iter_->value();
  }

  Status status() const override {
    if (!status_.ok()) {
      return status_;
    }
    return iter_->status();
  }

  void Next() override {
    if (prefetch_) {
      MutexLock l(&mutex_);
      assert(CurrentValid());
      Advance(true);
    } else {
      assert(iter_->Valid() && current_value_valid_);
      iter_->Next();
      MaybeIndirectValue();
    }
  }

  void Prev() override {
    if (prefetch_) {
      MutexLock l(&mutex_);
      assert(CurrentValid());
      Advance(false);
    } else {
      assert(iter_->Valid() && current_value_valid_);
      iter_->Prev();
      MaybeIndirectValue();
    }
  }

  void Seek(const Slice& target) override {
    if (prefetch_) {
      MutexLock l(&mutex_);
      iter_->Seek(target);
      Position(true);
    } else {
      iter_->Seek(target);
      MaybeIndirectValue();
    }
  }

  void SeekToFirst() override {
    if (prefetch_) {
      MutexLock l(&mutex_);
      iter_->SeekToFirst();
      Position(true);
    } else {
      iter_->SeekToFirst();
      MaybeIndirectValue();
    }
  }

  void SeekToLast() override {
    if (prefetch_) {
      MutexLock l(&mutex_);
      iter_->SeekToLast();
      Position(false);
    } else {
      iter_->SeekToLast();
      MaybeIndirectValue();
    }
  }

 private:
  // A heap-allocated Item remains alive while its value-log read is queued or
  // running. This lets seeks discard lookahead without invalidating a worker's
  // destination.
  struct Item {
    std::string key;
    std::string value;
    Status status;
    bool ready = false;
    bool valid = false;
    bool cancelled = false;
  };

  bool CurrentValid() const EXCLUSIVE_LOCKS_REQUIRED(mutex_) {
    mutex_.AssertHeld();
    return current_ != nullptr && current_->valid;
  }

  void SaveError(const Status& status) {
    if (status_.ok() && !status.ok()) {
      status_ = status;
    }
  }

  // Discard queued lookahead. Reads that have not started notice cancellation;
  // running reads finish into their own Item and only decrement pending_reads_.
  void CancelPrefetch() EXCLUSIVE_LOCKS_REQUIRED(mutex_) {
    mutex_.AssertHeld();
    for (const auto& item : prefetched_) {
      item->cancelled = true;
    }
    prefetched_.clear();
    cv_.SignalAll();
  }

  void ScheduleRead(const std::shared_ptr<Item>& item,
                    const ValueHandle& handle)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_) {
    mutex_.AssertHeld();

    // Repeated seeks cannot accumulate an unbounded number of obsolete jobs.
    while (pending_reads_ >= max_prefetch_) {
      cv_.Wait();
    }
    ++pending_reads_;

    env_->SubmitJob([this, item, handle]() {
      {
        MutexLock l(&mutex_);
        if (item->cancelled) {
          item->ready = true;
          --pending_reads_;
          cv_.SignalAll();
          return;
        }
      }

      std::string value;
      Status status = vlog_->Get(ReadOptions(), handle, &value);

      MutexLock l(&mutex_);
      if (!item->cancelled) {
        item->status = status;
        if (status.ok()) {
          item->value = std::move(value);
        } else {
          item->valid = false;
        }
      }
      item->ready = true;
      --pending_reads_;
      cv_.SignalAll();
    });
  }

  // Copy entries from DBIter into a bounded queue. Only the value-log lookup
  // is asynchronous; the underlying iterator is owned by the caller thread.
  void FillPrefetch() EXCLUSIVE_LOCKS_REQUIRED(mutex_) {
    mutex_.AssertHeld();
    while (prefetched_.size() < max_prefetch_ && iter_->Valid()) {
      std::shared_ptr<Item> item(new Item);
      item->key.assign(iter_->key().data(), iter_->key().size());
      item->value.assign(iter_->value().data(), iter_->value().size());
      item->valid = true;

      if (iter_->valueType() == kTypeValueHandle) {
        ValueHandle handle;
        Slice input(item->value);
        item->status = handle.DecodeFrom(&input);
        if (item->status.ok() && !input.empty()) {
          item->status = Status::Corruption(
              "trailing bytes in value-log handle");
        }
        if (!item->status.ok()) {
          item->valid = false;
          item->ready = true;
        } else {
          ScheduleRead(item, handle);
        }
      } else {
        item->ready = true;
      }

      prefetched_.emplace_back(std::move(item));
      if (forward_) {
        iter_->Next();
      } else {
        iter_->Prev();
      }
    }
  }

  void ConsumePrefetched() EXCLUSIVE_LOCKS_REQUIRED(mutex_) {
    mutex_.AssertHeld();
    if (prefetched_.empty()) {
      current_.reset();
      current_value_valid_ = false;
      SaveError(iter_->status());
      return;
    }

    std::shared_ptr<Item> item = prefetched_.front();
    while (!item->ready) {
      cv_.Wait();
    }
    prefetched_.pop_front();
    current_ = std::move(item);
    current_value_valid_ = current_->valid;
    SaveError(current_->status);

    if (!current_->valid) {
      CancelPrefetch();
    }
  }

  // iter_ is already at the requested position when this is called.
  void Position(bool forward) EXCLUSIVE_LOCKS_REQUIRED(mutex_) {
    mutex_.AssertHeld();
    CancelPrefetch();
    current_.reset();
    current_value_valid_ = false;
    forward_ = forward;
    positioned_ = true;
    FillPrefetch();
    ConsumePrefetched();
    if (CurrentValid()) {
      FillPrefetch();
    }
  }

  void Advance(bool forward) EXCLUSIVE_LOCKS_REQUIRED(mutex_) {
    mutex_.AssertHeld();
    assert(positioned_);

    if (forward_ != forward) {
      const std::string current_key = current_->key;
      CancelPrefetch();

      // Lookahead left iter_ beyond the current entry. Re-seek the stable user
      // key before taking one step in the new direction.
      iter_->Seek(current_key);
      if (iter_->Valid()) {
        if (forward) {
          iter_->Next();
        } else {
          iter_->Prev();
        }
      }
      forward_ = forward;
    }

    current_.reset();
    current_value_valid_ = false;
    FillPrefetch();
    ConsumePrefetched();
    if (CurrentValid()) {
      FillPrefetch();
    }
  }

  void MaybeIndirectValue() {
    current_value_valid_ = iter_->Valid();
    if (!iter_->Valid()) {
      isValueHandle_ = false;
      SaveError(iter_->status());
      return;
    }

    if (iter_->valueType() == kTypeValueHandle) {
      ValueHandle handle;
      Slice input(iter_->value());
      Status status = handle.DecodeFrom(&input);
      if (status.ok() && !input.empty()) {
        status = Status::Corruption("trailing bytes in value-log handle");
      }
      if (status.ok()) {
        status = vlog_->Get(ReadOptions(), handle, &indirect_value_);
      }
      if (!status.ok()) {
        current_value_valid_ = false;
        isValueHandle_ = false;
        SaveError(status);
        return;
      }
      isValueHandle_ = true;
    } else {
      isValueHandle_ = false;
    }
  }

  DBIter* iter_;
  ValueLogImpl* const vlog_;
  Env* const env_;
  std::string indirect_value_;
  bool current_value_valid_;
  bool isValueHandle_;
  const bool prefetch_;
  const size_t max_prefetch_;
  bool forward_ GUARDED_BY(mutex_);
  bool positioned_ GUARDED_BY(mutex_);
  size_t pending_reads_ GUARDED_BY(mutex_);
  Status status_;

  mutable port::Mutex mutex_;
  port::CondVar cv_ GUARDED_BY(mutex_);
  std::shared_ptr<Item> current_;
  std::deque<std::shared_ptr<Item>> prefetched_ GUARDED_BY(mutex_);
};

}  // anonymous namespace

Iterator* NewDBIterator(DBImpl* db, const Comparator* user_key_comparator,
                        Iterator* internal_iter, SequenceNumber sequence,
                        uint32_t seed) {
  return new DBIter(db, user_key_comparator, internal_iter, sequence, seed);
}

Iterator* NewBlobDBIterator(DBImpl* db, ValueLogImpl* vlog, Env* env,
                            const Comparator* user_key_comparator,
                            Iterator* internal_iter, SequenceNumber sequence,
                            uint32_t seed, bool prefetch, int max_prefetch) {
  DBIter* dbiter =
      new DBIter(db, user_key_comparator, internal_iter, sequence, seed);
  return new BlobDBIter(dbiter, vlog, env, prefetch, max_prefetch);
}

}  // namespace leveldb
