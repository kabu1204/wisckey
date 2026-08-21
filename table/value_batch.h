// Copyright (c) 2023. Chengye YU <yuchengye2013 AT outlook.com>
// SPDX-License-Identifier: BSD-3-Clause

#ifndef LEVELDB_VALUE_BATCH_H
#define LEVELDB_VALUE_BATCH_H

#include "db/dbformat.h"
#include <vector>

#include "leveldb/write_batch.h"

#include "table/format.h"

namespace leveldb {

class VLogBuilder;
class ValueLogImpl;

class ValueBatch {
 public:
  class Handler {
   public:
    virtual ~Handler() = default;
    // return true to continue iterating
    virtual bool operator()(const Slice& key, const Slice& value,
                            ValueHandle handle) = 0;
  };

  ValueBatch() : closed(false), num_entries(0) {}

  ~ValueBatch() = default;

  ValueBatch(const ValueBatch&) = default;
  ValueBatch& operator=(const ValueBatch&) = default;

  void Put(const Slice& key, const Slice& value);

  // The caller keeps key/value storage alive until this batch has been
  // written. This avoids copying large values out of the originating
  // WriteBatch merely to append them to the value log.
  void PutBorrowed(const Slice& key, const Slice& value);

  Status ToWriteBatch(WriteBatch* batch);

  const ValueHandle& Handle(size_t index) const {
    assert(index < num_entries);
    return borrowed_inline_ ? inline_borrowed_handle_ : handles_[index];
  }

  Status Iterate(Handler* handler);

  uint32_t NumEntries() const { return num_entries; }

  const char* data() const { return rep_.data(); }

  size_t size() const { return borrowed_ ? borrowed_size_ : rep_.size(); }

  static char* EncodeKey(char* ptr, const Slice& key);

  static void DecodeKey(const char* ptr, const char* end, Slice* key);

  static void PutKey(std::string* dst, const Slice& key);

  static bool GetKey(Slice* input, size_t n, Slice* key);

  static bool GetVLogRecord(Slice* input, Slice* key, Slice* value);

 private:
  struct BorrowedRecord {
    Slice key;
    Slice value;
  };

  friend class VLogBuilder;
  friend class VLogRWFile;
  friend class ValueLogImpl;

  void Finalize(uint32_t table, uint32_t offset) {
    assert(!closed);
    closed = true;
    if (borrowed_inline_) {
      inline_borrowed_handle_.table_ = table;
      inline_borrowed_handle_.offset_ += offset;
    }
    for (ValueHandle& handle : handles_) {
      handle.table_ = table;
      handle.offset_ += offset;
    }
  }

  const BorrowedRecord& BorrowedRecordAt(size_t index) const {
    assert(borrowed_ && index < num_entries);
    return borrowed_inline_ ? inline_borrowed_record_
                            : borrowed_records_[index];
  }

  bool closed;
  bool borrowed_{false};
  bool borrowed_inline_{false};
  std::string rep_;
  size_t borrowed_size_{0};
  BorrowedRecord inline_borrowed_record_;
  ValueHandle inline_borrowed_handle_;
  std::vector<BorrowedRecord> borrowed_records_;
  uint32_t num_entries;
  std::vector<ValueHandle> handles_;
};

}  // namespace leveldb

#endif  // LEVELDB_VALUE_BATCH_H
