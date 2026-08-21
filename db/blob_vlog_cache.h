// Copyright (c) 2023. Chengye YU <yuchengye2013 AT outlook.com>
// SPDX-License-Identifier: BSD-3-Clause

#ifndef LEVELDB_BLOB_VLOG_CACHE_H
#define LEVELDB_BLOB_VLOG_CACHE_H

#include "db/dbformat.h"
#include <cstdint>
#include <string>

#include "leveldb/cache.h"

#include "port/port.h"
#include "table/vlog.h"

namespace leveldb {

class Env;

class VLogCache {
 public:
  VLogCache(const std::string& dbname, const Options& options, int entries);

  VLogCache(const VLogCache&) = delete;
  VLogCache& operator=(const VLogCache&) = delete;

  ~VLogCache();

  Iterator* NewIterator(const ReadOptions& options, uint64_t file_number,
                        uint64_t file_size, VLogReader** reader = nullptr);

  Status Get(const ReadOptions& options, uint64_t file_number,
             uint64_t file_size, const Slice& k, void* arg,
             void (*handle_result)(void*, const Slice&, const Slice&));

  // Pins a reader so the backing file remains open while ValueLogImpl drops
  // its metadata lock for I/O.
  Status FindReader(uint64_t file_number, uint64_t file_size,
                    Cache::Handle** handle);

  Status Get(const ReadOptions& options, Cache::Handle* handle,
             uint32_t offset, uint32_t size, std::string* value);

  void Release(Cache::Handle* handle);

  // Evict any entry for the specified file number
  void Evict(uint64_t file_number);

 private:
  Env* const env_;
  const std::string dbname_;
  const Options options_;
  Cache* cache_;
};

}  // namespace leveldb

#endif  // LEVELDB_BLOB_VLOG_CACHE_H
