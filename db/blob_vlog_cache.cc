// Copyright (c) 2023. Chengye YU <yuchengye2013 AT outlook.com>
// SPDX-License-Identifier: BSD-3-Clause

#include "db/blob_vlog_cache.h"

#include "db/filename.h"

#include "leveldb/env.h"

#include "table/vlog.h"
#include "util/coding.h"

namespace leveldb {

struct TableAndFile {
  RandomAccessFile* file;
  VLogReader* table;
};

static void DeleteEntry(const Slice& key, void* value) {
  TableAndFile* tf = reinterpret_cast<TableAndFile*>(value);
  delete tf->table;
  delete tf->file;
  delete tf;
}

static void UnrefEntry(void* arg1, void* arg2) {
  Cache* cache = reinterpret_cast<Cache*>(arg1);
  Cache::Handle* h = reinterpret_cast<Cache::Handle*>(arg2);
  cache->Release(h);
}

VLogCache::VLogCache(const std::string& dbname,
                                 const Options& options, int entries)
    : env_(options.env),
      dbname_(dbname),
      options_(options),
      cache_(NewLRUCache(entries))
{}

VLogCache::~VLogCache() { delete cache_; }

Iterator* VLogCache::NewIterator(const ReadOptions& options,
                                       uint64_t file_number, uint64_t file_size,
                                       VLogReader** reader) {
  if(reader != nullptr){
    *reader = nullptr;
  }

  Cache::Handle* handle = nullptr;
  Status s = FindReader(file_number, file_size, &handle);
  if(!s.ok()){
    return NewErrorIterator(s);
  }

  VLogReader* t = reinterpret_cast<TableAndFile*>(cache_->Value(handle))->table;
  Iterator* result = t->NewIterator(options);
  result->RegisterCleanup(&UnrefEntry, cache_, handle);
  if(reader != nullptr) {
    *reader = t;
  }
  return result;
}

Status VLogCache::Get(const ReadOptions& options, uint64_t file_number,
                            uint64_t file_size, const Slice& k, void* arg,
                            void (*handle_result)(void*, const Slice&,
                                                  const Slice&)) {
  Cache::Handle* handle = nullptr;
  Status s = FindReader(file_number, file_size, &handle);
  if (s.ok()) {
    VLogReader* t = reinterpret_cast<TableAndFile*>(cache_->Value(handle))->table;
    s = t->InternalGet(options, k, arg, handle_result);
    cache_->Release(handle);
  }
  return s;
}

Status VLogCache::Get(const ReadOptions& options, Cache::Handle* handle,
                      uint32_t offset, uint32_t size, std::string* value) {
  if (handle == nullptr) {
    return Status::InvalidArgument("null value log cache handle");
  }
  VLogReader* reader =
      reinterpret_cast<TableAndFile*>(cache_->Value(handle))->table;
  return reader->Get(options, offset, size, value);
}

void VLogCache::Release(Cache::Handle* handle) {
  if (handle != nullptr) {
    cache_->Release(handle);
  }
}

void VLogCache::Evict(uint64_t file_number) {
    char buf[sizeof(file_number)];
    EncodeFixed64(buf, file_number);
    cache_->Erase(Slice(buf, sizeof(buf)));
}

Status VLogCache::FindReader(uint64_t file_number, uint64_t file_size,
                                  Cache::Handle** handle) {
  if (handle == nullptr) {
    return Status::InvalidArgument("null value log cache handle output");
  }
  Status s;
  char buf[sizeof(file_number)];
  EncodeFixed64(buf, file_number);
  Slice key(buf, sizeof(buf));
  *handle = cache_->Lookup(key);
  if(*handle == nullptr) {
    std::string fname = VLogFileName(dbname_, file_number);
    RandomAccessFile* file = nullptr;
    VLogReader* reader = nullptr;
    s = env_->NewRandomAccessFile(fname, &file);
    if(s.ok()){
      s = VLogReader::Open(options_, file, file_size, &reader);
    }

    if(!s.ok()) {
      delete file;
    } else {
      TableAndFile* tf = new TableAndFile;
      tf->file = file;
      tf->table = reader;
      *handle = cache_->Insert(key, tf, 1, &DeleteEntry);
    }
  }

  return s;
}
}  // namespace leveldb
