// Copyright (c) 2023. Chengye YU <yuchengye2013 AT outlook.com>
// SPDX-License-Identifier: BSD-3-Clause

#ifndef LEVELDB_VLOG_ITER_H
#define LEVELDB_VLOG_ITER_H

#include "leveldb/env.h"
#include "leveldb/iterator.h"

#include "table/format.h"

namespace leveldb {

class VLogReaderIterator : public Iterator {
 private:
  friend class VLogReader;
  const RandomAccessFile* const file_;
  const uint32_t limit_;
  uint32_t current_{0};
  Status status_;
  char* buf_;
  size_t buf_size_;
  std::string key_;
  Slice value_;
  uint32_t parsed_entry_size_;
  bool valid_{false};

  bool ParseCurrentEntry(uint32_t size = 0);

 public:
  VLogReaderIterator(const RandomAccessFile* file, uint32_t limit,
                     uint32_t buf_size = 1024);

  ~VLogReaderIterator() override;

  bool Valid() const override;

  void SeekToFirst() override;

  void SeekToLast() override;

  void Seek(const Slice& target) override;

  void Next() override;

  void Prev() override {
    status_ = Status::NotSupported(
        "vlog iterator does not support iterating reversely");
    valid_ = false;
  }

  void GetValueHandle(ValueHandle* handle) const;

  uint32_t record_offset() const { return current_; }

  uint32_t record_size() const { return parsed_entry_size_; }

  Slice key() const override { return {key_.data(), key_.size()}; }

  Slice value() const override { return value_; }

  Status status() const override { return status_; }
};

}  // namespace leveldb

#endif  // LEVELDB_VLOG_ITER_H
