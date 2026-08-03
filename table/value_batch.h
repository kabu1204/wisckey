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
  void PutBorrowed(const Slice& key, const Slice& value);

  Status ToWriteBatch(WriteBatch* batch);

  const std::vector<ValueHandle>& Handles() const { return handles_; }

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
  friend class ValueLogImpl;

  void Finalize(uint32_t table, uint32_t offset) {
    assert(!closed);
    closed = true;
    for (auto& handle : handles_) {
      handle.table_ = table;
      handle.offset_ += offset;
    }
  }

  bool closed;
  bool borrowed_ = false;
  std::string rep_;
  size_t borrowed_size_ = 0;
  std::vector<BorrowedRecord> borrowed_records_;
  uint32_t num_entries;
  std::vector<ValueHandle> handles_;
};

}  // namespace leveldb

#endif  // LEVELDB_VALUE_BATCH_H
