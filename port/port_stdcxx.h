// Copyright (c) 2018 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
// Modifications Copyright 2023 Chengye YU <yuchengye2013 AT outlook.com>.

#ifndef STORAGE_LEVELDB_PORT_PORT_STDCXX_H_
#define STORAGE_LEVELDB_PORT_PORT_STDCXX_H_

// port/port_config.h availability is automatically detected via __has_include
// in newer compilers. If LEVELDB_HAS_PORT_CONFIG_H is defined, it overrides the
// configuration detection.
#if defined(LEVELDB_HAS_PORT_CONFIG_H)

#if LEVELDB_HAS_PORT_CONFIG_H
#include "port/port_config.h"
#endif  // LEVELDB_HAS_PORT_CONFIG_H

#elif defined(__has_include)

#if __has_include("port/port_config.h")
#include "port/port_config.h"
#endif  // __has_include("port/port_config.h")

#endif  // defined(LEVELDB_HAS_PORT_CONFIG_H)

#if HAVE_CRC32C
#include <crc32c/crc32c.h>
#endif  // HAVE_CRC32C
#if HAVE_SNAPPY
#include <snappy.h>
#endif  // HAVE_SNAPPY

#include <atomic>
#include <cassert>
#include <condition_variable>  // NOLINT
#include <cstddef>
#include <cstdint>
#include <mutex>  // NOLINT
#include <string>

#include "port/thread_annotations.h"

namespace leveldb {
namespace port {

class CondVar;

/*
 * TODO: is it too expensive to use a virtual spinlock?
 */
class LOCKABLE RWLock {
 public:
  RWLock() = default;
  ~RWLock() = default;
  RWLock(const RWLock&) = delete;
  RWLock& operator=(const RWLock&) = delete;

  virtual void RLock() SHARED_LOCK_FUNCTION() = 0;

  virtual void WLock() EXCLUSIVE_LOCK_FUNCTION() = 0;

  virtual void RUnlock() UNLOCK_FUNCTION() = 0;

  virtual void WUnlock() UNLOCK_FUNCTION() = 0;

  virtual void AssertRLockHeld() ASSERT_SHARED_LOCK() {}
  virtual void AssertWLockHeld() ASSERT_EXCLUSIVE_LOCK() {}
};

class LOCKABLE RWSpinLock : public RWLock {
 public:
  RWSpinLock() : x(0) {}
  ~RWSpinLock() = default;
  RWSpinLock(const RWSpinLock&) = delete;
  RWSpinLock& operator=(const RWSpinLock&) = delete;

  void RLock() SHARED_LOCK_FUNCTION() override {
    while (!TryRLock())
      ;
  }

  void WLock() EXCLUSIVE_LOCK_FUNCTION() override {
    while (!TryWLock())
      ;
  }

  void RUnlock() UNLOCK_FUNCTION() override {
    x.fetch_sub(0b10, std::memory_order_release);
  }

  void WUnlock() UNLOCK_FUNCTION() override {
    x.fetch_and(~(0b01), std::memory_order_release);
  }

  bool TryRLock() SHARED_TRYLOCK_FUNCTION(true) {
    if (x.fetch_add(0b10, std::memory_order_acquire) & 0b01) {
      x.fetch_sub(0b10, std::memory_order_release);
      return false;
    }
    return true;
  }

  bool TryWLock() EXCLUSIVE_TRYLOCK_FUNCTION(true) {
    uint32_t expected = 0;
    return x.compare_exchange_strong(expected, 0b01, std::memory_order_acq_rel);
  }

 private:
  /*
   * |000000|0| => no readers/writers
   * |000000|1| => writer exclusive
   * |000001|0| => 1 reader
   * |000100|0| => 8 readers
   */
  std::atomic<uint32_t> x;
};

// Thinly wraps std::mutex.
class LOCKABLE Mutex {
 public:
  Mutex() = default;
  ~Mutex() = default;

  Mutex(const Mutex&) = delete;
  Mutex& operator=(const Mutex&) = delete;

  void Lock() EXCLUSIVE_LOCK_FUNCTION() { mu_.lock(); }
  void Unlock() UNLOCK_FUNCTION() { mu_.unlock(); }
  void AssertHeld() ASSERT_EXCLUSIVE_LOCK() {}

 private:
  friend class CondVar;
  std::mutex mu_;
};

// Thinly wraps std::condition_variable.
class CondVar {
 public:
  explicit CondVar(Mutex* mu) : mu_(mu) { assert(mu != nullptr); }
  ~CondVar() = default;

  CondVar(const CondVar&) = delete;
  CondVar& operator=(const CondVar&) = delete;

  void Wait() {
    /* Assumes the calling thread already holds a non-shared lock
     * (i.e., a lock acquired by lock, try_lock, try_lock_for,
     * or try_lock_until) on m.
     */
    std::unique_lock<std::mutex> lock(mu_->mu_, std::adopt_lock);
    cv_.wait(lock);
    // disassociates the associated mutex without unlocking (i.e., releasing ownership of) it
    lock.release();
  }
  void Signal() { cv_.notify_one(); }
  void SignalAll() { cv_.notify_all(); }

 private:
  std::condition_variable cv_;
  Mutex* const mu_;
};

/*
 * Read-Write Lock based on CondVar
 * TODO:
 *  1. use notify_one() to avoid thundering herd
 *  2. use atomic<uint32_t> to avoid acquiring mutex when unlocking.
 */
class LOCKABLE RWMutex : public RWLock {
 public:
  RWMutex() : readers_(0), writer_(false), waiting_writers_(0) {}
  ~RWMutex() = default;
  RWMutex(const RWMutex&) = delete;
  RWMutex& operator=(const RWMutex&) = delete;

  void WLock() EXCLUSIVE_LOCK_FUNCTION() override {
    std::unique_lock<std::mutex> lk(mu_);
    ++waiting_writers_;
    cv_.wait(lk, [this]() { return !writer_ && readers_ == 0; });
    --waiting_writers_;
    writer_ = true;
  }

  void WUnlock() UNLOCK_FUNCTION() override {
    std::unique_lock<std::mutex> lk(mu_);
    writer_ = false;
    cv_.notify_all();
  }

  void RLock() SHARED_LOCK_FUNCTION() override {
    std::unique_lock<std::mutex> lk(mu_);
    cv_.wait(lk,
             [this]() { return !writer_ && waiting_writers_ == 0; });
    ++readers_;
  }

  void RUnlock() UNLOCK_FUNCTION() override {
    std::unique_lock<std::mutex> lk(mu_);
    assert(readers_ > 0);
    --readers_;
    if (readers_ == 0) cv_.notify_all();
  }

 private:
  std::condition_variable cv_;
  std::mutex mu_;
  uint32_t readers_;
  bool writer_;
  uint32_t waiting_writers_;
};

inline bool Snappy_Compress(const char* input, size_t length,
                            std::string* output) {
#if HAVE_SNAPPY
  output->resize(snappy::MaxCompressedLength(length));
  size_t outlen;
  snappy::RawCompress(input, length, &(*output)[0], &outlen);
  output->resize(outlen);
  return true;
#else
  // Silence compiler warnings about unused arguments.
  (void)input;
  (void)length;
  (void)output;
#endif  // HAVE_SNAPPY

  return false;
}

inline bool Snappy_GetUncompressedLength(const char* input, size_t length,
                                         size_t* result) {
#if HAVE_SNAPPY
  return snappy::GetUncompressedLength(input, length, result);
#else
  // Silence compiler warnings about unused arguments.
  (void)input;
  (void)length;
  (void)result;
  return false;
#endif  // HAVE_SNAPPY
}

inline bool Snappy_Uncompress(const char* input, size_t length, char* output) {
#if HAVE_SNAPPY
  return snappy::RawUncompress(input, length, output);
#else
  // Silence compiler warnings about unused arguments.
  (void)input;
  (void)length;
  (void)output;
  return false;
#endif  // HAVE_SNAPPY
}

inline bool GetHeapProfile(void (*func)(void*, const char*, int), void* arg) {
  // Silence compiler warnings about unused arguments.
  (void)func;
  (void)arg;
  return false;
}

inline uint32_t AcceleratedCRC32C(uint32_t crc, const char* buf, size_t size) {
#if HAVE_CRC32C
  return ::crc32c::Extend(crc, reinterpret_cast<const uint8_t*>(buf), size);
#else
  // Silence compiler warnings about unused arguments.
  (void)crc;
  (void)buf;
  (void)size;
  return 0;
#endif  // HAVE_CRC32C
}

}  // namespace port
}  // namespace leveldb

#endif  // STORAGE_LEVELDB_PORT_PORT_STDCXX_H_
