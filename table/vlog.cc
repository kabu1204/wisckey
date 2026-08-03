// Copyright (c) 2023. Chengye YU <yuchengye2013 AT outlook.com>
// SPDX-License-Identifier: BSD-3-Clause
//
// Entry in the .vlog file:
//  key_len:    VarUint32
//  value_len:  VarUint32
//  key:        uint8[key_len]
//  value:      uint8[value_len]

#include "table/vlog.h"

#include "db/write_batch_internal.h"

#include "table/value_batch.h"

namespace leveldb {

struct VLogBuilder::Rep {
  Rep(const Options& opt, AppendableRandomAccessFile* f)
      : options(opt), file(f) {}

  Options options;
  Status status;
  AppendableRandomAccessFile* file;
  bool closed{false};
  uint32_t num_entries{0};
  uint32_t offset{0};

  char buf[1024];
  std::string compressed;
};

VLogBuilder::VLogBuilder(const Options& options,
                         AppendableRandomAccessFile* file, bool reuse,
                         uint32_t offset, uint32_t num_entries)
    : rep_(new Rep(options, file))
{
  if (reuse) {
    rep_->offset = offset;
    rep_->num_entries = num_entries;
  }
}

VLogBuilder::~VLogBuilder() {
  assert(rep_->closed);
  delete rep_;
}

// TODO: should not be used
void VLogBuilder::Add(const Slice& key, const Slice& value, ValueHandle* handle) {
  assert(!rep_->closed);
  if(!ok()) return;
  uint32_t size = key.size() + value.size();
  // TODO: directly append to file
  char *buf = (size + 10) > 1024 ? new char[size+10]: rep_->buf;

  char *ptr = EncodeVarint32(buf, key.size());
  ptr = EncodeVarint32(ptr, value.size());
  size += (ptr - buf);
  std::memcpy(ptr, key.data(), key.size());
  std::memcpy(ptr + key.size(), value.data(), value.size());
  rep_->status = rep_->file->Append(Slice(buf, size));
  if(buf != rep_->buf){
    delete[] buf;
  }
  if(!ok()){
    return;
  }

  if (handle != nullptr) {
    handle->offset_ = rep_->offset;
    handle->size_ = size;
  }
  rep_->offset += size;
  rep_->num_entries++;
}

void VLogBuilder::AddBatch(const ValueBatch* batch) {
  rep_->status = rep_->file->Append(Slice(batch->data(), batch->size()));
  if (!ok()) return;

  rep_->offset += batch->size();
  rep_->num_entries += batch->NumEntries();
}

void VLogBuilder::Flush() {
  assert(!rep_->closed);
  rep_->status = rep_->file->Flush();
}

Status VLogBuilder::status() const { return rep_->status; }

Status VLogBuilder::Finish() {
  Flush();
  assert(!rep_->closed);
  rep_->closed = true;
  return rep_->status;
}

void VLogBuilder::Abandon() {
  assert(!rep_->closed);
  rep_->closed = true;
}

uint64_t VLogBuilder::NumEntries() const { return rep_->num_entries; }

uint64_t VLogBuilder::FileSize() const { return rep_->offset; }

uint64_t VLogBuilder::Offset() const { return rep_->offset; }

struct VLogReader::Rep {
  Rep(const Options& opt, RandomAccessFile* f, uint32_t lim)
      : file(f),
        option(opt),
        limit(lim)
  {}

  Options option;
  Status status;
  uint32_t limit;
  RandomAccessFile* file;
};

static inline const char* DecodeEntry(const char* p, const char* limit,
                                      uint32_t* key_len, uint32_t* value_len) {
  if (limit - p < 2) return nullptr;
  *key_len = reinterpret_cast<const uint8_t*>(p)[0];
  if ((*key_len) < 128) {
    p++;
  } else {
    if ((p = GetVarint32Ptr(p, limit, key_len)) == nullptr) return nullptr;
  }
  if ((p = GetVarint32Ptr(p, limit, value_len)) == nullptr) return nullptr;

  if (static_cast<uint32_t>(limit - p) < (*key_len + *value_len)) {
    return nullptr;
  }
  return p;
}

// When file is appendable, file_size should be the actually len of the file.
Status VLogReader::Open(const Options& options, RandomAccessFile* file,
                        uint64_t file_size, VLogReader** reader) {
  if(reader != nullptr) {
    *reader = nullptr;
  }

  Rep *rep = new Rep(options, file, file_size);
  *reader = new VLogReader(rep);

  return Status::OK();
}

VLogReader::~VLogReader() {
  delete rep_;
}

Status VLogReader::Get(const ReadOptions& options, uint32_t offset,
                       uint32_t size, std::string* value) const {
  if (offset >= rep_->limit || size > rep_->limit - offset) {
    return Status::InvalidArgument("value log offset is out of limit");
  }

  char stack_scratch[512];
  thread_local std::string reusable_scratch;
  char* scratch = stack_scratch;
  if (size > sizeof(stack_scratch)) {
    if (reusable_scratch.size() < size) {
      reusable_scratch.resize(size);
    }
    scratch = &reusable_scratch[0];
  }

  Slice result;
  Status s = rep_->file->Read(offset, size, &result, scratch);
  if (!s.ok()) {
    return s;
  }

  uint32_t key_len;
  uint32_t value_len;
  const char* p =
      DecodeEntry(result.data(), result.data() + result.size(), &key_len,
                  &value_len);
  if (p == nullptr) {
    return Status::Corruption("corrupted vlog entry");
  }
  value->assign(p + key_len, value_len);
  return Status::OK();
}

bool VLogReaderIterator::ParseCurrentEntry(uint32_t size) {
  if (current_ >= limit_) return false;
  Slice result;
  char* scratch;
  const char* p;
  uint32_t key_len, val_len;

  if (size == 0) {
    status_ = file_->Read(current_, std::min<size_t>(10, limit_ - current_),
                          &result, buf_);  // TODO: opt
    if (!status_.ok()) {
      return false;
    }
    p = DecodeEntry(result.data(), result.data() - current_ + limit_, &key_len,
                    &val_len);
    if (p == nullptr) {
      status_ = Status::Corruption("corrupted vlog entry");
      return false;
    }
    if (key_len == 0) {
      status_ = Status::OK();
      return false;
    }

    size = key_len + val_len + (p - result.data());
    if (key_len + val_len > buf_size_) {
      delete[] buf_;
      buf_size_ = key_len + val_len;
      buf_ = new char[buf_size_];
    }
    scratch = buf_;
    status_ = file_->Read(current_ + (p - result.data()), key_len + val_len,
                          &result, scratch);
    p = result.data();
  } else {
    if (size > buf_size_) {
      delete[] buf_;
      buf_size_ = size;
      buf_ = new char[buf_size_];
    }
    scratch = buf_;
    status_ = file_->Read(current_, size, &result, scratch);
    p = DecodeEntry(result.data(), result.data() + result.size(), &key_len,
                    &val_len);
  }

  if (!status_.ok()) {
    return false;
  }
  if (p == nullptr) {
    status_ = Status::Corruption("corrupted vlog entry");
    return false;
  }
  key_.assign(p, key_len);
  value_ = Slice(p + key_len, val_len);
  parsed_entry_size_ = size;
  return true;
}

VLogReaderIterator::VLogReaderIterator(const RandomAccessFile* file,
                                       uint32_t limit, uint32_t buf_size)
    : file_(file),
      limit_(limit),
      buf_size_(buf_size),
      buf_(new char[buf_size]) {}

VLogReaderIterator::~VLogReaderIterator() { delete[] buf_; }

bool VLogReaderIterator::Valid() const { return valid_; }

void VLogReaderIterator::SeekToFirst() {
  current_ = 0;
  valid_ = ParseCurrentEntry();
}

void VLogReaderIterator::SeekToLast() {
  uint32_t prev;
  if (current_ >= limit_) {
    SeekToFirst();
  }
  prev = current_;
  for (Next(); Valid(); Next()) {
    prev = current_;
  }
  current_ = prev;
  valid_ = ParseCurrentEntry();
}

void VLogReaderIterator::Seek(const Slice& target) {
  ValueHandle handle;
  Slice input = target;
  status_ = handle.DecodeFrom(&input);
  current_ = handle.offset_;
  valid_ = ParseCurrentEntry(handle.size_);
}

void VLogReaderIterator::Next() {
  assert(current_ < limit_);
  current_ += parsed_entry_size_;
  valid_ = ParseCurrentEntry();
}

void VLogReaderIterator::GetValueHandle(ValueHandle* handle) const {
  handle->offset_ = current_;
  handle->size_ = parsed_entry_size_;
}

VLogReaderIterator* VLogReader::NewIterator(const ReadOptions& option) const {
  return new VLogReaderIterator(rep_->file, rep_->limit);
}

void VLogReader::IncreaseOffset(uint32_t new_offset) {
  assert(new_offset >= rep_->limit);
  rep_->limit = new_offset;
}

Status VLogReader::InternalGet(const ReadOptions& option, const Slice& key, void* arg,
                               void (*handle_result)(void*, const Slice&,
                                                     const Slice&)) const {
  Status s;
  Iterator* iter = NewIterator(option);
  iter->Seek(key);
  if (iter->Valid()) {
    // TODO: do we need block cache here?
    (*handle_result)(arg, iter->key(), iter->value());
  }
  if (s.ok()) {
    s = iter->status();
  }
  delete iter;
  return s;
}

bool VLogReader::Validate(uint64_t* offset, uint64_t* num_entries) {
  VLogReaderIterator* iter = NewIterator(ReadOptions());
  assert(iter != nullptr);
  *offset = 0;
  *num_entries = 0;
  uint64_t n = 0;
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    n++;
  }
  *offset = iter->current_;
  *num_entries = n;
  delete iter;
  return *offset == rep_->limit;
}

char* ValueBatch::EncodeKey(char* ptr, const Slice& key) {
  std::memcpy(ptr, key.data(), key.size());
  return ptr + key.size();
}

void ValueBatch::PutKey(std::string* dst, const Slice& key) {
  dst->append(key.data(), key.size());
}

void ValueBatch::DecodeKey(const char* ptr, const char* end, Slice* key) {
  *key = Slice(ptr, end - ptr);
}

bool ValueBatch::GetKey(Slice* input, size_t n, Slice* key) {
  if (n > input->size()) {
    return false;
  }
  *key = Slice(input->data(), n);
  input->remove_prefix(n);
  return true;
}

bool ValueBatch::GetVLogRecord(Slice* input, Slice* key, Slice* value) {
  const char* p = input->data();
  const char* limit = input->data() + input->size();
  uint32_t key_size, value_size;
  const char* q = DecodeEntry(p, limit, &key_size, &value_size);
  if (q != nullptr && (limit - q >= key_size + value_size)) {
    input->remove_prefix(q - p);
    ValueBatch::GetKey(input, key_size, key);
    if (value != nullptr) {
      *value = Slice(input->data(), value_size);
    }
    input->remove_prefix(value_size);
    return true;
  }
  return false;
}

// should be called after SetSequence()
void ValueBatch::Put(const Slice& key, const Slice& value) {
  assert(!closed);
  uint32_t off = rep_.size();
  PutVarint32(&rep_, key.size());
  PutVarint32(&rep_, value.size());
  ValueBatch::PutKey(&rep_, key);
  rep_.append(value.data(), value.size());
  handles_.emplace_back(0, 0, off, rep_.size() - off);
  num_entries++;
}

class ToWriteBatchHandler : public ValueBatch::Handler {
 public:
  ~ToWriteBatchHandler() override = default;
  bool operator()(const Slice& key, const Slice& value,
                  ValueHandle handle) override {
    handle.EncodeTo(&handle_encoding);
    WriteBatchInternal::Put(batch, key, handle_encoding, kTypeValueHandle);
    return true;
  }
  std::string handle_encoding;
  WriteBatch* batch;
};

Status ValueBatch::ToWriteBatch(WriteBatch* batch) {
  assert(closed);
  assert(num_entries == handles_.size());
  ToWriteBatchHandler handler;
  handler.batch = batch;

  return Iterate(&handler);
}

Status ValueBatch::Iterate(ValueBatch::Handler* handler) {
  assert(num_entries == handles_.size());
  uint32_t found = 0;
  Slice user_key, value;
  std::string handle_encoding;
  Slice input(rep_.data(), rep_.size());

  while (!input.empty()) {
    if (ValueBatch::GetVLogRecord(&input, &user_key, &value)) {
      if (!(*handler)(user_key, value, handles_[found])) {
        return Status::OK();
      }
      found++;
    }
  }
  if (found != num_entries) {
    return Status::Corruption("corrupted ValueBatch");
  }
  return Status::OK();
}

}  // namespace leveldb