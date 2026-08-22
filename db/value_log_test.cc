// Copyright (c) 2023. Chengye YU <yuchengye2013 AT outlook.com>
// SPDX-License-Identifier: BSD-3-Clause

#include "db/blob_vlog_impl.h"
#include "db/filename.h"
#include "db/write_batch_internal.h"
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstring>
#include <deque>
#include <limits>
#include <memory>
#include <mutex>
#include <random>
#include <set>
#include <thread>
#include <unordered_map>

#include "leveldb/env.h"

#include "table/format.h"
#include "util/sync_point.h"

#include "gtest/gtest.h"

using namespace leveldb;

#ifdef NDEBUG
#define REQUIRE_TEST_SYNC_POINTS()                                      \
  GTEST_SKIP() << "sync-point hooks are disabled in release builds"
#else
#define REQUIRE_TEST_SYNC_POINTS() \
  do {                             \
  } while (false)
#endif

// SyncPoint is a process-wide singleton. Tests register callbacks with stack
// arguments, so every exit path (including an ASSERT_* early return) must
// unregister them before the next test starts.
class ScopedGCSyncPoints {
 public:
  ScopedGCSyncPoints() { Clear(); }
  ~ScopedGCSyncPoints() { Clear(); }

  ScopedGCSyncPoints(const ScopedGCSyncPoints&) = delete;
  ScopedGCSyncPoints& operator=(const ScopedGCSyncPoints&) = delete;

 private:
  static void Clear() {
    TEST_SYNC_POINT_CLEAR("GC.AfterCollect");
    TEST_SYNC_POINT_CLEAR("GC.BeforeBackground");
    TEST_SYNC_POINT_CLEAR("GC.RunGC.InCriticalSection");
    TEST_SYNC_POINT_CLEAR("GC.Rewrite.AfterBatch");
    TEST_SYNC_POINT_CLEAR("GC.Rewrite.AfterLSMRewrite");
    TEST_SYNC_POINT_CLEAR("GC.Rewrite.AfterValueRewrite");
  }
};

static void CleanDir(Env* const env, const std::string& dir) {
  std::vector<std::string> fnames;
  if (!env->GetChildren(dir, &fnames).ok()) {
    return;
  }
  for (auto& filename : fnames) {
    env->RemoveFile(DBFilePath(dir, filename));
  }
  env->RemoveDir(dir);
}

uint32_t SizeOfVariant32(uint32_t v) {
  uint32_t size = 1;
  while (v >>= 7) {
    size++;
  }
  return size;
}

uint64_t SizeOf(const Slice& key, const Slice& val) {
  return SizeOfVariant32(key.size()) + SizeOfVariant32(val.size()) +
         key.size() + val.size();
}

static std::string IteratorKey(int index) {
  std::string number = std::to_string(index);
  return "key" + std::string(4 - number.size(), '0') + number;
}

static std::string IteratorValue(int index) {
  return "value-" + std::to_string(index) + std::string(128, 'x');
}

static void PutIteratorEntries(BlobDB* db, int count) {
  for (int i = 0; i < count; ++i) {
    ASSERT_TRUE(
        db->Put(WriteOptions(), IteratorKey(i), IteratorValue(i)).ok());
  }
}

static void ExpectIteratorEntry(Iterator* iter, int index) {
  ASSERT_TRUE(iter->Valid()) << iter->status().ToString();
  EXPECT_EQ(IteratorKey(index), iter->key().ToString());
  EXPECT_EQ(IteratorValue(index), iter->value().ToString());
}

static std::vector<uint64_t> ReadOnlyVLogNumbers(Env* env,
                                                  const std::string& dbname) {
  std::vector<std::string> children;
  EXPECT_TRUE(env->GetChildren(dbname, &children).ok());
  std::set<uint64_t> numbers;
  for (const std::string& child : children) {
    uint64_t number = 0;
    FileType type;
    if (ParseFileName(child, &number, &type) && type == kVLogFile) {
      numbers.insert(number);
    }
  }
  if (!numbers.empty()) {
    numbers.erase(*numbers.rbegin());  // current writable vlog
  }
  return {numbers.begin(), numbers.end()};
}

TEST(VLOG_TEST, DBWrapperPrefetchEmptyIterator) {
  Options options;
  options.create_if_missing = true;
  options.blob_background_read_threads = 2;
  const std::string dbname("testdb-prefetch-empty");
  CleanDir(options.env, dbname);

  BlobDB* raw_db = nullptr;
  ASSERT_TRUE(BlobDB::Open(options, dbname, &raw_db).ok());
  std::unique_ptr<BlobDB> db(raw_db);

  ReadOptions read_options;
  read_options.blob_prefetch = true;
  std::unique_ptr<Iterator> iter(db->NewIterator(read_options));
  EXPECT_FALSE(iter->Valid());
  EXPECT_TRUE(iter->status().ok());

  iter->SeekToFirst();
  EXPECT_FALSE(iter->Valid());
  EXPECT_TRUE(iter->status().ok());
  iter->SeekToLast();
  EXPECT_FALSE(iter->Valid());
  EXPECT_TRUE(iter->status().ok());
  iter->Seek("key");
  EXPECT_FALSE(iter->Valid());
  EXPECT_TRUE(iter->status().ok());

  iter.reset();
  db.reset();
  CleanDir(options.env, dbname);
}

TEST(VLOG_TEST, DBWrapperPrefetchLifecycle) {
  Options options;
  options.create_if_missing = true;
  options.blob_value_size_threshold = 16;
  options.blob_max_file_size = 8 << 20;
  options.blob_background_read_threads = 3;
  const std::string dbname("testdb-prefetch-lifecycle");
  CleanDir(options.env, dbname);

  BlobDB* raw_db = nullptr;
  ASSERT_TRUE(BlobDB::Open(options, dbname, &raw_db).ok());
  std::unique_ptr<BlobDB> db(raw_db);
  const int num_entries = 64;
  PutIteratorEntries(db.get(), num_entries);

  ReadOptions read_options;
  read_options.blob_prefetch = true;
  std::unique_ptr<Iterator> iter(db->NewIterator(read_options));
  EXPECT_FALSE(iter->Valid());
  EXPECT_TRUE(iter->status().ok());

  // Repeated seeks discard live lookahead. Alternating Next/Prev checks both
  // direction changes from the same logical entry.
  for (int round = 0; round < 100; ++round) {
    const int index = 2 + (round * 17) % (num_entries - 4);
    iter->Seek(IteratorKey(index));
    ExpectIteratorEntry(iter.get(), index);
    iter->Next();
    ExpectIteratorEntry(iter.get(), index + 1);
    iter->Prev();
    ExpectIteratorEntry(iter.get(), index);
    iter->Prev();
    ExpectIteratorEntry(iter.get(), index - 1);
    iter->Next();
    ExpectIteratorEntry(iter.get(), index);
  }

  int index = 0;
  for (iter->SeekToFirst(); iter->Valid(); iter->Next(), ++index) {
    ExpectIteratorEntry(iter.get(), index);
  }
  EXPECT_EQ(num_entries, index);
  EXPECT_TRUE(iter->status().ok()) << iter->status().ToString();

  index = num_entries - 1;
  for (iter->SeekToLast(); iter->Valid(); iter->Prev(), --index) {
    ExpectIteratorEntry(iter.get(), index);
  }
  EXPECT_EQ(-1, index);
  EXPECT_TRUE(iter->status().ok()) << iter->status().ToString();

  iter->Seek("zzzz");
  EXPECT_FALSE(iter->Valid());
  iter->SeekToFirst();
  ExpectIteratorEntry(iter.get(), 0);

  // Destruction must cancel/drain outstanding lookahead without relying on a
  // detached dispatch thread touching an already-destroyed iterator.
  iter.reset();
  iter.reset(db->NewIterator(read_options));
  iter->SeekToFirst();
  ExpectIteratorEntry(iter.get(), 0);
  iter.reset();

  db.reset();
  CleanDir(options.env, dbname);
}

TEST(VLOG_TEST, DBWrapperPrefetchZeroDepthFallsBack) {
  Options options;
  options.create_if_missing = true;
  options.blob_value_size_threshold = 16;
  options.blob_background_read_threads = 0;
  const std::string dbname("testdb-prefetch-zero-depth");
  CleanDir(options.env, dbname);

  BlobDB* raw_db = nullptr;
  ASSERT_TRUE(BlobDB::Open(options, dbname, &raw_db).ok());
  std::unique_ptr<BlobDB> db(raw_db);
  PutIteratorEntries(db.get(), 8);

  ReadOptions read_options;
  read_options.blob_prefetch = true;
  std::unique_ptr<Iterator> iter(db->NewIterator(read_options));
  EXPECT_FALSE(iter->Valid());
  iter->SeekToFirst();
  for (int index = 0; index < 8; ++index) {
    ExpectIteratorEntry(iter.get(), index);
    if (index + 1 < 8) {
      iter->Next();
    }
  }
  iter->SeekToLast();
  ExpectIteratorEntry(iter.get(), 7);
  iter->Prev();
  ExpectIteratorEntry(iter.get(), 6);
  EXPECT_TRUE(iter->status().ok()) << iter->status().ToString();

  iter.reset();
  db.reset();
  CleanDir(options.env, dbname);
}

TEST(VLOG_TEST, DBWrapperPrefetchIter) {
  Options options;
  Status s;
  options.env->NewStdLogger(&options.info_log);
  options.create_if_missing = true;
  options.blob_max_file_size = 8 << 20;
  options.blob_value_size_threshold = 512;
  options.blob_background_read_threads = 8;
  std::string dbname("testdb");
  std::string value;
  CleanDir(options.env, dbname);
  int num_entries = 400000;
  int reverse0 = 234567;
  int reverse1 = 123456;

  int fill = std::to_string(num_entries).size();

  BlobDB* db;
  BlobDB::Open(options, dbname, &db);

  std::map<std::string, std::string> kvmap;
  std::random_device rd;
  std::mt19937 mt(rd());
  std::uniform_int_distribution<int> dist(1024, 2048);
  for (int i = 0; i < num_entries; ++i) {
    std::string n = std::to_string(i);
    std::string key = "key" + std::string(fill - n.size(), '0') + n;
    key = std::to_string(std::hash<std::string>{}(key));
    std::string val = "value" + std::string(dist(mt), 'x');
    kvmap[key] = val;
    s = db->Put(WriteOptions(), key, val);
    ASSERT_TRUE(s.ok());
  }

  ReadOptions opt;
  opt.blob_prefetch = true;
  Iterator* iter = db->NewIterator(opt);
  auto it = kvmap.begin();
  int d = 0;
  for (iter->SeekToFirst(); iter->Valid(); iter->Next(), it++) {
    Slice key = iter->key();
    Slice val = iter->value();
    ASSERT_EQ(it->first, key.ToString());
    ASSERT_EQ(it->second, val);
    d++;
    if (d == reverse0) {
      break;
    }
  }

  for (; iter->Valid(); iter->Prev(), it = std::prev(it)) {
    Slice key = iter->key();
    Slice val = iter->value();
    ASSERT_EQ(it->first, key.ToString());
    ASSERT_EQ(it->second, val);
    d--;
    if (d == reverse1) {
      break;
    }
  }

  for (; iter->Valid(); iter->Next(), it++) {
    Slice key = iter->key();
    Slice val = iter->value();
    ASSERT_EQ(it->first, key.ToString());
    ASSERT_EQ(it->second, val);
    d++;
  }

  ASSERT_EQ(d, num_entries);

  auto reverse_it = kvmap.rbegin();
  d = 0;
  for (iter->SeekToLast(); iter->Valid(); iter->Prev(), reverse_it++) {
    Slice key = iter->key();
    Slice val = iter->value();
    ASSERT_EQ(reverse_it->first, key.ToString());
    ASSERT_EQ(reverse_it->second, val);
    d++;
  }
  ASSERT_EQ(d, num_entries);

  delete iter;
  delete db;

  CleanDir(options.env, dbname);
}

TEST(VLOG_TEST, DBWrapperGC_FailAfterLSMRewrite) {
  REQUIRE_TEST_SYNC_POINTS();
  ScopedGCSyncPoints sync_points;
  Options options;
  Status s;
  options.env->NewStdLogger(&options.info_log);
  options.create_if_missing = true;
  options.blob_max_file_size = 8 << 20;
  options.blob_value_size_threshold = 256;
  std::string dbname("testdb");
  std::string value;
  CleanDir(options.env, dbname);
  int num_entries = 100000;

  BlobDB* db;
  BlobDB::Open(options, dbname, &db);

  std::unordered_map<std::string, std::string> kvmap;
  std::vector<std::string> rewrites;

  auto validate_fn = [&db, &kvmap, &value]() {
    Status s;
    for (const auto& p : kvmap) {
      std::string key = p.first;
      s = db->Get(ReadOptions(), key, &value);
      ASSERT_TRUE(s.ok());
      ASSERT_EQ(kvmap[key], value);
    }
  };

  size_t size = 0;
  for (int i = 0; i < num_entries; ++i) {
    std::string key = "key" + std::to_string(i);
    std::string val = "value" + std::string(256, 'x');
    kvmap[key] = val;
    if (size <= options.blob_max_file_size) {
      rewrites.emplace_back(key);
      size += SizeOf(key, val);
    }
    s = db->Put(WriteOptions(), key, val);
    ASSERT_TRUE(s.ok());
  }

  std::random_device rd;
  std::mt19937 g(rd());
  std::shuffle(rewrites.begin(), rewrites.end(), g);

  int d = 0;
  for (auto& key : rewrites) {
    std::string val = "NEWvalue" + std::string(256, 'x');
    s = db->Put(WriteOptions(), key, val);
    ASSERT_TRUE(s.ok());

    kvmap[key] = val;
    d++;
    if (d > rewrites.size() / 2 + 1) {
      break;
    }
  }

  struct sync_point_arg {
    BlobDB* db;
    std::unordered_map<std::string, std::string>* kvs;
  };
  sync_point_arg myarg = {.db = db, .kvs = &kvmap};
  auto sync_point_cb = +[](void* arg) {
    BlobDB* db = reinterpret_cast<sync_point_arg*>(arg)->db;
    auto* kvmap = reinterpret_cast<sync_point_arg*>(arg)->kvs;
    std::string value;
    Status s;
    for (const auto& p : *kvmap) {
      std::string key = p.first;
      s = db->Get(ReadOptions(), key, &value);
      assert(s.ok());
      assert((*kvmap)[key] == value);
    }
    return true;
  };

  TEST_SYNC_POINT_CLEAR("GC.AfterCollect");

  TEST_SYNC_POINT_CLEAR("GC.Rewrite.AfterValueRewrite");
  TEST_SYNC_POINT_CLEAR("GC.Rewrite.AfterLSMRewrite");

  TEST_SYNC_POINT_ARG("GC.Rewrite.AfterLSMRewrite", &myarg);
  TEST_SYNC_POINT_CALLBACK("GC.Rewrite.AfterLSMRewrite", sync_point_cb);

  db->ManualGC(0);  // discard ratio ~50%
  db->WaitVLogGC();
  ASSERT_TRUE(db->VLogBGError().ok());

  delete db;
  BlobDB::Open(options, dbname, &db);

  // Put one more record to expire the old vlog file
  db->Put(WriteOptions(), "OneMoreKey", "value");
  db->RemoveObsoleteBlob();

  delete db;
  BlobDB::Open(options, dbname, &db);

  validate_fn();

  printf("%s", reinterpret_cast<BlobDB*>(db)->DebugString().c_str());
  delete db;
  CleanDir(options.env, dbname);
}

TEST(VLOG_TEST, DBWrapperGC_FailAfterValueRewrite) {
  REQUIRE_TEST_SYNC_POINTS();
  ScopedGCSyncPoints sync_points;
  Options options;
  Status s;
  options.env->NewStdLogger(&options.info_log);
  options.create_if_missing = true;
  options.blob_max_file_size = 8 << 20;
  options.blob_value_size_threshold = 256;
  std::string dbname("testdb");
  std::string value;
  CleanDir(options.env, dbname);
  int num_entries = 100000;

  BlobDB* db;
  BlobDB::Open(options, dbname, &db);

  std::unordered_map<std::string, std::string> kvmap;
  std::vector<std::string> rewrites;

  auto validate_fn = [&db, &kvmap, &value]() {
    Status s;
    for (const auto& p : kvmap) {
      std::string key = p.first;
      s = db->Get(ReadOptions(), key, &value);
      ASSERT_TRUE(s.ok());
      ASSERT_EQ(kvmap[key], value);
    }
  };

  size_t size = 0;
  for (int i = 0; i < num_entries; ++i) {
    std::string key = "key" + std::to_string(i);
    std::string val = "value" + std::string(256, 'x');
    kvmap[key] = val;
    if (size <= options.blob_max_file_size) {
      rewrites.emplace_back(key);
      size += SizeOf(key, val);
    }
    s = db->Put(WriteOptions(), key, val);
    ASSERT_TRUE(s.ok());
  }

  std::random_device rd;
  std::mt19937 g(rd());
  std::shuffle(rewrites.begin(), rewrites.end(), g);

  int d = 0;
  for (auto& key : rewrites) {
    std::string val = "NEWvalue" + std::string(256, 'x');
    s = db->Put(WriteOptions(), key, val);
    ASSERT_TRUE(s.ok());

    kvmap[key] = val;
    d++;
    if (d > rewrites.size() / 2 + 1) {
      break;
    }
  }

  struct sync_point_arg {
    BlobDB* db;
    std::unordered_map<std::string, std::string>* kvs;
  };
  sync_point_arg myarg = {.db = db, .kvs = &kvmap};
  auto sync_point_cb = +[](void* arg) {
    BlobDB* db = reinterpret_cast<sync_point_arg*>(arg)->db;
    auto* kvmap = reinterpret_cast<sync_point_arg*>(arg)->kvs;
    std::string value;
    Status s;
    for (const auto& p : *kvmap) {
      std::string key = p.first;
      s = db->Get(ReadOptions(), key, &value);
      assert(s.ok());
      assert((*kvmap)[key] == value);
    }
    return true;
  };

  TEST_SYNC_POINT_CLEAR("GC.AfterCollect");

  TEST_SYNC_POINT_CLEAR("GC.Rewrite.AfterValueRewrite");
  TEST_SYNC_POINT_CLEAR("GC.Rewrite.AfterLSMRewrite");

  TEST_SYNC_POINT_ARG("GC.Rewrite.AfterValueRewrite", &myarg);
  TEST_SYNC_POINT_CALLBACK("GC.Rewrite.AfterValueRewrite", sync_point_cb);

  db->ManualGC(0);  // discard ratio ~50%
  db->WaitVLogGC();
  ASSERT_TRUE(db->VLogBGError().ok());

  delete db;
  BlobDB::Open(options, dbname, &db);

  validate_fn();

  TEST_SYNC_POINT_CLEAR("GC.Rewrite.AfterValueRewrite");
  TEST_SYNC_POINT_CLEAR("GC.Rewrite.AfterLSMRewrite");

  db->ManualGC(7);  // discard ratio ~100%
  db->WaitVLogGC();
  ASSERT_TRUE(db->VLogBGError().ok());

  // Put one more record to expire the old vlog file
  db->Put(WriteOptions(), "OneMoreKey", "value");
  db->RemoveObsoleteBlob();

  delete db;
  BlobDB::Open(options, dbname, &db);

  validate_fn();

  printf("%s", reinterpret_cast<BlobDB*>(db)->DebugString().c_str());
  delete db;
  CleanDir(options.env, dbname);
}

TEST(VLOG_TEST, DBWrapperGCBoundedRewrite) {
  REQUIRE_TEST_SYNC_POINTS();
  ScopedGCSyncPoints sync_points;
  Options options;
  options.create_if_missing = true;
  options.blob_max_file_size = 4 << 20;
  options.blob_value_size_threshold = 256;
  options.blob_gc_size_discard_threshold = 20;
  options.blob_gc_num_discard_threshold = 20;
  options.blob_gc_interval = 0;
  const std::string dbname("testdb_gc_bounded_rewrite");
  CleanDir(options.env, dbname);

  std::atomic<int> flushed_batches{0};
  auto count_batch = +[](void* arg) {
    reinterpret_cast<std::atomic<int>*>(arg)->fetch_add(
        1, std::memory_order_relaxed);
    return false;
  };
  TEST_SYNC_POINT_ARG("GC.Rewrite.AfterBatch", &flushed_batches);
  TEST_SYNC_POINT_CALLBACK("GC.Rewrite.AfterBatch", count_batch);

  BlobDB* db = nullptr;
  ASSERT_TRUE(BlobDB::Open(options, dbname, &db).ok());

  constexpr int kEntries = 5000;
  constexpr int kOverwrites = 1500;
  const std::string old_value(1024, 'o');
  const std::string new_value(1024, 'n');
  for (int i = 0; i < kEntries; ++i) {
    ASSERT_TRUE(db->Put(WriteOptions(), "key" + std::to_string(i), old_value)
                    .ok());
  }
  for (int i = 0; i < kOverwrites; ++i) {
    ASSERT_TRUE(db->Put(WriteOptions(), "key" + std::to_string(i), new_value)
                    .ok());
  }

  db->ManualGC(0);
  db->WaitVLogGC();
  ASSERT_TRUE(db->VLogBGError().ok());
  // More than one 1 MiB batch proves the rewrite did not retain the complete
  // live portion of this 4 MiB source file in one ValueBatch.
  ASSERT_GE(flushed_batches.load(std::memory_order_relaxed), 2);

  std::string value;
  for (int i = 0; i < kEntries; ++i) {
    ASSERT_TRUE(db->Get(ReadOptions(), "key" + std::to_string(i), &value).ok());
    ASSERT_EQ(i < kOverwrites ? new_value : old_value, value);
  }

  delete db;
  TEST_SYNC_POINT_CLEAR("GC.Rewrite.AfterBatch");
  CleanDir(options.env, dbname);
}

TEST(VLOG_TEST, GCReplacementAtMaximumFileNumberCanBeCollected) {
  Options options;
  options.create_if_missing = true;
  options.blob_max_file_size = 2048;
  options.blob_value_size_threshold = 256;
  options.blob_gc_size_discard_threshold = 1;
  options.blob_gc_num_discard_threshold = 1;
  options.blob_gc_interval = 0;
  const std::string dbname("testdb_gc_maximum_replacement");
  CleanDir(options.env, dbname);

  BlobDB* db = nullptr;
  ASSERT_TRUE(BlobDB::Open(options, dbname, &db).ok());

  const std::string stale_value(900, 's');
  const std::string live_value(900, 'l');
  const std::string rollover_value(900, 'r');
  ASSERT_TRUE(db->Put(WriteOptions(), "stale", stale_value).ok());
  ASSERT_TRUE(db->Put(WriteOptions(), "live", live_value).ok());
  ASSERT_TRUE(db->Put(WriteOptions(), "rollover", rollover_value).ok());
  ASSERT_TRUE(db->Put(WriteOptions(), "stale", "stale-inline").ok());

  auto list_vlogs = [&]() {
    std::set<uint64_t> numbers;
    std::vector<std::string> children;
    const Status s = options.env->GetChildren(dbname, &children);
    EXPECT_TRUE(s.ok()) << s.ToString();
    for (const std::string& child : children) {
      uint64_t number = 0;
      FileType type;
      if (ParseFileName(child, &number, &type) && type == kVLogFile) {
        numbers.insert(number);
      }
    }
    return numbers;
  };

  const std::set<uint64_t> before_gc = list_vlogs();
  ASSERT_GE(before_gc.size(), 2U);
  const uint64_t source_number = *before_gc.begin();
  const uint64_t maximum_before_gc = *before_gc.rbegin();
  ASSERT_LT(source_number, maximum_before_gc);

  // Rewriting the partially stale source allocates a read-only replacement
  // above the current writable vlog, making the replacement the maximum file
  // number known to ValueLogImpl.
  db->ManualGC(source_number);
  db->WaitVLogGC();
  ASSERT_TRUE(db->VLogBGError().ok()) << db->VLogBGError().ToString();

  const std::set<uint64_t> after_first_gc = list_vlogs();
  auto replacement_it = after_first_gc.upper_bound(maximum_before_gc);
  ASSERT_NE(after_first_gc.end(), replacement_it);
  const uint64_t replacement_number = *replacement_it;
  ++replacement_it;
  ASSERT_EQ(after_first_gc.end(), replacement_it);
  ASSERT_EQ(replacement_number, *after_first_gc.rbegin());
  const std::string replacement_name =
      VLogFileName(dbname, replacement_number);
  ASSERT_TRUE(options.env->FileExists(replacement_name));

  // An inline overwrite neither appends to nor rolls the current vlog, so the
  // replacement remains the maximum file number when selected for the next GC.
  ASSERT_TRUE(db->Put(WriteOptions(), "live", "live-inline").ok());
  ASSERT_EQ(after_first_gc, list_vlogs());

  db->ManualGC(replacement_number);
  db->WaitVLogGC();
  ASSERT_TRUE(db->VLogBGError().ok()) << db->VLogBGError().ToString();
  EXPECT_FALSE(options.env->FileExists(replacement_name));

  std::string value;
  ASSERT_TRUE(db->Get(ReadOptions(), "live", &value).ok());
  EXPECT_EQ("live-inline", value);
  ASSERT_TRUE(db->Get(ReadOptions(), "rollover", &value).ok());
  EXPECT_EQ(rollover_value, value);

  delete db;
  CleanDir(options.env, dbname);
}

TEST(VLOG_TEST, DBWrapperRunGCVisitsInitialFilesOnce) {
  REQUIRE_TEST_SYNC_POINTS();
  ScopedGCSyncPoints sync_points;
  Options options;
  options.create_if_missing = true;
  options.blob_max_file_size = 64 << 10;
  options.blob_value_size_threshold = 256;
  options.blob_gc_size_discard_threshold = 20;
  options.blob_gc_num_discard_threshold = 20;
  options.blob_gc_interval = 0;
  options.blob_max_background_gc = 2;
  const std::string dbname("testdb_gc_full_pass");
  CleanDir(options.env, dbname);

  BlobDB* db = nullptr;
  ASSERT_TRUE(BlobDB::Open(options, dbname, &db).ok());
  const std::string value(8 << 10, 'v');
  const std::string new_value(8 << 10, 'n');
  for (int i = 0; i < 30; ++i) {
    ASSERT_TRUE(db->Put(WriteOptions(), "key" + std::to_string(i), value).ok());
  }
  for (int i = 0; i < 30; i += 2) {
    ASSERT_TRUE(
        db->Put(WriteOptions(), "key" + std::to_string(i), new_value).ok());
  }

  std::vector<std::string> children;
  ASSERT_TRUE(options.env->GetChildren(dbname, &children).ok());
  int vlog_files = 0;
  for (const std::string& child : children) {
    uint64_t number = 0;
    FileType type;
    if (ParseFileName(child, &number, &type) && type == kVLogFile) {
      ++vlog_files;
    }
  }
  ASSERT_GT(vlog_files, 1);
  const int expected_sources = vlog_files - 1;  // exclude current writable vlog

  std::atomic<int> background_calls{0};
  auto count_call = +[](void* arg) {
    reinterpret_cast<std::atomic<int>*>(arg)->fetch_add(
        1, std::memory_order_relaxed);
    return false;
  };
  TEST_SYNC_POINT_ARG("GC.BeforeBackground", &background_calls);
  TEST_SYNC_POINT_CALLBACK("GC.BeforeBackground", count_call);

  // The original source files contain enough overwritten records to create
  // replacement vlogs. The replacement files must not expand this pass.
  ASSERT_TRUE(db->RunVLogGC().ok());
  ASSERT_EQ(expected_sources,
            background_calls.load(std::memory_order_relaxed));

  std::string result;
  for (int i = 0; i < 30; ++i) {
    ASSERT_TRUE(
        db->Get(ReadOptions(), "key" + std::to_string(i), &result).ok());
    ASSERT_EQ(i % 2 == 0 ? new_value : value, result);
  }

  TEST_SYNC_POINT_CLEAR("GC.BeforeBackground");
  delete db;
  CleanDir(options.env, dbname);
}

TEST(VLOG_TEST, DBWrapperConcurrentRunGCSerializesAndRestartsTimer) {
  REQUIRE_TEST_SYNC_POINTS();
  ScopedGCSyncPoints sync_points;
  Options options;
  options.create_if_missing = true;
  options.blob_gc_interval = 1;
  options.blob_background_read_threads = 0;
  const std::string dbname("testdb_gc_concurrent_full_pass");
  CleanDir(options.env, dbname);

  struct RunGCState {
    std::atomic<int> attempted{0};
    std::atomic<int> entered{0};
    std::atomic<int> active{0};
    std::atomic<int> max_active{0};
    std::atomic<bool> release{false};
  } state;
  auto hold_critical_section = +[](void* arg) {
    auto* state = reinterpret_cast<RunGCState*>(arg);
    state->entered.fetch_add(1, std::memory_order_relaxed);
    const int active =
        state->active.fetch_add(1, std::memory_order_acq_rel) + 1;
    int maximum = state->max_active.load(std::memory_order_relaxed);
    while (maximum < active &&
           !state->max_active.compare_exchange_weak(
               maximum, active, std::memory_order_relaxed)) {
    }
    while (!state->release.load(std::memory_order_acquire)) {
      std::this_thread::yield();
    }
    state->active.fetch_sub(1, std::memory_order_acq_rel);
    return false;
  };
  TEST_SYNC_POINT_ARG("GC.RunGC.InCriticalSection", &state);
  TEST_SYNC_POINT_CALLBACK("GC.RunGC.InCriticalSection",
                           hold_critical_section);

  std::atomic<int> background_calls{0};
  auto count_background = +[](void* arg) {
    reinterpret_cast<std::atomic<int>*>(arg)->fetch_add(
        1, std::memory_order_relaxed);
    return false;
  };
  TEST_SYNC_POINT_ARG("GC.BeforeBackground", &background_calls);
  TEST_SYNC_POINT_CALLBACK("GC.BeforeBackground", count_background);

  BlobDB* db = nullptr;
  ASSERT_TRUE(BlobDB::Open(options, dbname, &db).ok());

  constexpr int kCallers = 8;
  std::atomic<int> ready{0};
  std::atomic<bool> start{false};
  std::vector<Status> statuses(kCallers);
  std::vector<std::thread> callers;
  callers.reserve(kCallers);
  for (int caller = 0; caller < kCallers; ++caller) {
    callers.emplace_back([&, caller] {
      ready.fetch_add(1, std::memory_order_release);
      while (!start.load(std::memory_order_acquire)) {
        std::this_thread::yield();
      }
      state.attempted.fetch_add(1, std::memory_order_release);
      statuses[caller] = db->RunVLogGC();
    });
  }

  while (ready.load(std::memory_order_acquire) != kCallers) {
    std::this_thread::yield();
  }
  start.store(true, std::memory_order_release);
  for (int i = 0;
       i < 5000 && state.entered.load(std::memory_order_acquire) == 0; ++i) {
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }

  const bool first_entered =
      state.entered.load(std::memory_order_acquire) != 0;
  if (first_entered) {
    while (state.attempted.load(std::memory_order_acquire) != kCallers) {
      std::this_thread::yield();
    }
    // Keep the first call inside the critical section long enough for every
    // other caller to contend. They must remain outside the sync point.
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    EXPECT_EQ(1, state.entered.load(std::memory_order_acquire));
  }
  state.release.store(true, std::memory_order_release);
  for (std::thread& caller : callers) {
    caller.join();
  }

  EXPECT_TRUE(first_entered) << "RunVLogGC did not reach its critical section";
  EXPECT_EQ(1, state.max_active.load(std::memory_order_acquire));
  EXPECT_EQ(kCallers, state.entered.load(std::memory_order_acquire));
  for (const Status& status : statuses) {
    EXPECT_TRUE(status.ok()) << status.ToString();
  }

  const int calls_before_restart =
      background_calls.load(std::memory_order_acquire);
  for (int i = 0;
       i < 500 && background_calls.load(std::memory_order_acquire) ==
                      calls_before_restart;
       ++i) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  EXPECT_GT(background_calls.load(std::memory_order_acquire),
            calls_before_restart)
      << "periodic GC timer did not restart after concurrent RunVLogGC calls";

  delete db;
  CleanDir(options.env, dbname);
}

TEST(VLOG_TEST, BackgroundGCParallelismOptionValidation) {
  Options options;
  options.create_if_missing = true;
  options.blob_gc_interval = 0;
  const std::string dbname("testdb_gc_parallel_option");
  CleanDir(options.env, dbname);

  BlobDB* db = nullptr;
  options.blob_max_background_gc = 0;
  EXPECT_TRUE(BlobDB::Open(options, dbname, &db).IsInvalidArgument());
  EXPECT_EQ(nullptr, db);

  options.blob_max_background_gc = 3;
  EXPECT_TRUE(BlobDB::Open(options, dbname, &db).IsInvalidArgument());
  EXPECT_EQ(nullptr, db);

  options.blob_max_background_gc = 1;
  ASSERT_TRUE(BlobDB::Open(options, dbname, &db).ok());
  delete db;
  CleanDir(options.env, dbname);
}

TEST(VLOG_TEST, BackgroundGCParallelismGuard) {
  EXPECT_FALSE(CanRunVLogGCConcurrently(0, 3));
  EXPECT_FALSE(CanRunVLogGCConcurrently(3, 0));
  EXPECT_FALSE(CanRunVLogGCConcurrently(3, 3));
  EXPECT_TRUE(CanRunVLogGCConcurrently(3, 4));
}

TEST(VLOG_TEST, DBWrapperRunsDistinctGCFilesInParallel) {
  REQUIRE_TEST_SYNC_POINTS();
  ScopedGCSyncPoints sync_points;
  Options options;
  options.create_if_missing = true;
  options.blob_max_file_size = 32 << 10;
  options.blob_value_size_threshold = 256;
  options.blob_gc_size_discard_threshold = 0;
  options.blob_gc_num_discard_threshold = 0;
  options.blob_gc_interval = 0;
  options.blob_max_background_gc = 2;
  const std::string dbname("testdb_gc_parallel_distinct");
  CleanDir(options.env, dbname);

  BlobDB* db = nullptr;
  ASSERT_TRUE(BlobDB::Open(options, dbname, &db).ok());
  const std::string value(8 << 10, 'v');
  for (int i = 0; i < 16; ++i) {
    ASSERT_TRUE(db->Put(WriteOptions(), "key" + std::to_string(i), value).ok());
  }

  std::vector<std::string> children;
  ASSERT_TRUE(options.env->GetChildren(dbname, &children).ok());
  std::set<uint64_t> file_numbers;
  for (const std::string& child : children) {
    uint64_t number = 0;
    FileType type;
    if (ParseFileName(child, &number, &type) && type == kVLogFile) {
      file_numbers.insert(number);
    }
  }
  ASSERT_GE(file_numbers.size(), 3U);
  file_numbers.erase(*file_numbers.rbegin());  // current writable vlog
  auto source = file_numbers.begin();
  const uint64_t first = *source++;
  const uint64_t second = *source;

  struct ConcurrentGC {
    std::atomic<int> calls{0};
    std::atomic<int> active{0};
    std::atomic<int> max_active{0};
    std::atomic<bool> release{false};
  } state;
  auto block_both = +[](void* arg) {
    auto* state = reinterpret_cast<ConcurrentGC*>(arg);
    state->calls.fetch_add(1, std::memory_order_release);
    const int active =
        state->active.fetch_add(1, std::memory_order_acq_rel) + 1;
    int maximum = state->max_active.load(std::memory_order_relaxed);
    while (maximum < active &&
           !state->max_active.compare_exchange_weak(
               maximum, active, std::memory_order_relaxed)) {
    }
    while (!state->release.load(std::memory_order_acquire)) {
      std::this_thread::yield();
    }
    state->active.fetch_sub(1, std::memory_order_acq_rel);
    return false;
  };
  TEST_SYNC_POINT_ARG("GC.BeforeBackground", &state);
  TEST_SYNC_POINT_CALLBACK("GC.BeforeBackground", block_both);

  db->ManualGC(first);
  db->ManualGC(second);
  for (int i = 0;
       i < 5000 && state.calls.load(std::memory_order_acquire) < 2; ++i) {
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  EXPECT_EQ(2, state.calls.load(std::memory_order_acquire));
  EXPECT_EQ(2, state.max_active.load(std::memory_order_acquire));

  state.release.store(true, std::memory_order_release);
  db->WaitVLogGC();
  EXPECT_TRUE(db->VLogBGError().ok()) << db->VLogBGError().ToString();

  TEST_SYNC_POINT_CLEAR("GC.BeforeBackground");
  delete db;
  CleanDir(options.env, dbname);
}

TEST(VLOG_TEST, DBWrapperQueuesDuplicateActiveGCSource) {
  REQUIRE_TEST_SYNC_POINTS();
  ScopedGCSyncPoints sync_points;
  Options options;
  options.create_if_missing = true;
  options.blob_max_file_size = 32 << 10;
  options.blob_value_size_threshold = 256;
  options.blob_gc_size_discard_threshold = 100;
  options.blob_gc_num_discard_threshold = 100;
  options.blob_gc_interval = 0;
  options.blob_max_background_gc = 2;
  const std::string dbname("testdb_gc_parallel_duplicate_source");
  CleanDir(options.env, dbname);

  BlobDB* db = nullptr;
  ASSERT_TRUE(BlobDB::Open(options, dbname, &db).ok());
  const std::string value(8 << 10, 'v');
  for (int i = 0; i < 16; ++i) {
    ASSERT_TRUE(db->Put(WriteOptions(), "key" + std::to_string(i), value).ok());
  }
  const std::vector<uint64_t> sources =
      ReadOnlyVLogNumbers(options.env, dbname);
  ASSERT_GE(sources.size(), 2U);

  struct BlockingGC {
    std::atomic<int> calls{0};
    std::atomic<int> active{0};
    std::atomic<int> max_active{0};
    std::atomic<bool> first_entered{false};
    std::atomic<bool> release_first{false};
  } state;
  auto block_first = +[](void* arg) {
    auto* state = reinterpret_cast<BlockingGC*>(arg);
    const int active =
        state->active.fetch_add(1, std::memory_order_acq_rel) + 1;
    int maximum = state->max_active.load(std::memory_order_relaxed);
    while (maximum < active &&
           !state->max_active.compare_exchange_weak(
               maximum, active, std::memory_order_relaxed)) {
    }
    const int call = state->calls.fetch_add(1, std::memory_order_acq_rel);
    if (call == 0) {
      state->first_entered.store(true, std::memory_order_release);
      while (!state->release_first.load(std::memory_order_acquire)) {
        std::this_thread::yield();
      }
    }
    state->active.fetch_sub(1, std::memory_order_acq_rel);
    return false;
  };
  TEST_SYNC_POINT_ARG("GC.BeforeBackground", &state);
  TEST_SYNC_POINT_CALLBACK("GC.BeforeBackground", block_first);

  db->ManualGC(sources[0]);
  for (int i = 0;
       i < 5000 && !state.first_entered.load(std::memory_order_acquire); ++i) {
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  ASSERT_TRUE(state.first_entered.load(std::memory_order_acquire));

  db->ManualGC(sources[0]);
  std::this_thread::sleep_for(std::chrono::milliseconds(50));
  EXPECT_EQ(1, state.calls.load(std::memory_order_acquire));
  EXPECT_EQ(1, state.max_active.load(std::memory_order_acquire));

  state.release_first.store(true, std::memory_order_release);
  db->WaitVLogGC();
  EXPECT_EQ(2, state.calls.load(std::memory_order_acquire));
  EXPECT_EQ(1, state.max_active.load(std::memory_order_acquire));

  TEST_SYNC_POINT_CLEAR("GC.BeforeBackground");
  delete db;
  CleanDir(options.env, dbname);
}

TEST(VLOG_TEST, DBWrapperRunGCUsesTwoWorkers) {
  REQUIRE_TEST_SYNC_POINTS();
  ScopedGCSyncPoints sync_points;
  Options options;
  options.create_if_missing = true;
  options.blob_max_file_size = 32 << 10;
  options.blob_value_size_threshold = 256;
  options.blob_gc_size_discard_threshold = 100;
  options.blob_gc_num_discard_threshold = 100;
  options.blob_gc_interval = 0;
  options.blob_max_background_gc = 2;
  const std::string dbname("testdb_gc_parallel_full_pass");
  CleanDir(options.env, dbname);

  BlobDB* db = nullptr;
  ASSERT_TRUE(BlobDB::Open(options, dbname, &db).ok());
  const std::string value(8 << 10, 'v');
  for (int i = 0; i < 16; ++i) {
    ASSERT_TRUE(db->Put(WriteOptions(), "key" + std::to_string(i), value).ok());
  }
  ASSERT_GE(ReadOnlyVLogNumbers(options.env, dbname).size(), 2U);

  struct BlockingGC {
    std::atomic<int> calls{0};
    std::atomic<int> active{0};
    std::atomic<int> max_active{0};
    std::atomic<bool> release{false};
  } state;
  auto block_initial_jobs = +[](void* arg) {
    auto* state = reinterpret_cast<BlockingGC*>(arg);
    state->calls.fetch_add(1, std::memory_order_release);
    const int active =
        state->active.fetch_add(1, std::memory_order_acq_rel) + 1;
    int maximum = state->max_active.load(std::memory_order_relaxed);
    while (maximum < active &&
           !state->max_active.compare_exchange_weak(
               maximum, active, std::memory_order_relaxed)) {
    }
    while (!state->release.load(std::memory_order_acquire)) {
      std::this_thread::yield();
    }
    state->active.fetch_sub(1, std::memory_order_acq_rel);
    return false;
  };
  TEST_SYNC_POINT_ARG("GC.BeforeBackground", &state);
  TEST_SYNC_POINT_CALLBACK("GC.BeforeBackground", block_initial_jobs);

  Status run_status;
  std::thread runner([&] { run_status = db->RunVLogGC(); });
  for (int i = 0;
       i < 5000 && state.calls.load(std::memory_order_acquire) < 2; ++i) {
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  EXPECT_EQ(2, state.calls.load(std::memory_order_acquire));
  EXPECT_EQ(2, state.max_active.load(std::memory_order_acquire));
  state.release.store(true, std::memory_order_release);
  runner.join();
  EXPECT_TRUE(run_status.ok()) << run_status.ToString();

  TEST_SYNC_POINT_CLEAR("GC.BeforeBackground");
  delete db;
  CleanDir(options.env, dbname);
}

TEST(VLOG_TEST, ParallelGCPreservesForegroundWritesAndSnapshot) {
  REQUIRE_TEST_SYNC_POINTS();
  ScopedGCSyncPoints sync_points;
  Options options;
  options.create_if_missing = true;
  options.blob_max_file_size = 24 << 10;
  options.blob_value_size_threshold = 256;
  options.blob_gc_size_discard_threshold = 0;
  options.blob_gc_num_discard_threshold = 0;
  options.blob_gc_interval = 0;
  options.blob_max_background_gc = 2;
  const std::string dbname("testdb_gc_parallel_foreground_snapshot");
  CleanDir(options.env, dbname);

  BlobDB* db = nullptr;
  ASSERT_TRUE(BlobDB::Open(options, dbname, &db).ok());
  const std::string old_value(8 << 10, 'o');
  constexpr int kEntries = 12;
  for (int i = 0; i < kEntries; ++i) {
    ASSERT_TRUE(db->Put(WriteOptions(), "key" + std::to_string(i), old_value)
                    .ok());
  }
  const std::vector<uint64_t> sources =
      ReadOnlyVLogNumbers(options.env, dbname);
  ASSERT_GE(sources.size(), 2U);
  const Snapshot* snapshot = db->GetSnapshot();

  struct ForegroundMutation {
    BlobDB* db;
    std::atomic<int> calls{0};
    Status status;
  } mutation;
  mutation.db = db;
  auto mutate_after_collect = +[](void* arg) {
    auto* mutation = reinterpret_cast<ForegroundMutation*>(arg);
    if (mutation->calls.fetch_add(1, std::memory_order_acq_rel) != 0) {
      return false;
    }
    for (int i = 0; i < kEntries; ++i) {
      Status s = i % 2 == 0
                     ? mutation->db->Put(WriteOptions(),
                                         "key" + std::to_string(i),
                                         "new-" + std::to_string(i))
                     : mutation->db->Delete(WriteOptions(),
                                            "key" + std::to_string(i));
      if (!s.ok()) {
        mutation->status = s;
        break;
      }
    }
    return false;
  };
  TEST_SYNC_POINT_ARG("GC.AfterCollect", &mutation);
  TEST_SYNC_POINT_CALLBACK("GC.AfterCollect", mutate_after_collect);

  db->ManualGC(sources[0]);
  db->ManualGC(sources[1]);
  db->WaitVLogGC();
  EXPECT_TRUE(mutation.status.ok()) << mutation.status.ToString();
  ASSERT_GE(mutation.calls.load(std::memory_order_acquire), 1);

  ReadOptions snapshot_read;
  snapshot_read.snapshot = snapshot;
  std::string value;
  ASSERT_TRUE(db->Get(snapshot_read, "key0", &value).ok());
  EXPECT_EQ(old_value, value);
  for (size_t i = 0; i < 2; ++i) {
    EXPECT_TRUE(options.env->FileExists(VLogFileName(dbname, sources[i])));
  }

  db->ReleaseSnapshot(snapshot);
  db->RemoveObsoleteBlob();
  for (size_t i = 0; i < 2; ++i) {
    EXPECT_FALSE(options.env->FileExists(VLogFileName(dbname, sources[i])));
  }
  for (int i = 0; i < kEntries; ++i) {
    Status s = db->Get(ReadOptions(), "key" + std::to_string(i), &value);
    if (i % 2 == 0) {
      ASSERT_TRUE(s.ok()) << s.ToString();
      EXPECT_EQ("new-" + std::to_string(i), value);
    } else {
      EXPECT_TRUE(s.IsNotFound()) << s.ToString();
    }
  }

  TEST_SYNC_POINT_CLEAR("GC.AfterCollect");
  delete db;
  CleanDir(options.env, dbname);
}

TEST(VLOG_TEST, ParallelGCShutdownWaitsForBothWorkers) {
  REQUIRE_TEST_SYNC_POINTS();
  ScopedGCSyncPoints sync_points;
  Options options;
  options.create_if_missing = true;
  options.blob_max_file_size = 32 << 10;
  options.blob_value_size_threshold = 256;
  options.blob_gc_interval = 0;
  options.blob_max_background_gc = 2;
  const std::string dbname("testdb_gc_parallel_shutdown");
  CleanDir(options.env, dbname);

  BlobDB* db = nullptr;
  ASSERT_TRUE(BlobDB::Open(options, dbname, &db).ok());
  const std::string value(8 << 10, 'v');
  for (int i = 0; i < 16; ++i) {
    ASSERT_TRUE(db->Put(WriteOptions(), "key" + std::to_string(i), value).ok());
  }
  const std::vector<uint64_t> sources =
      ReadOnlyVLogNumbers(options.env, dbname);
  ASSERT_GE(sources.size(), 2U);

  struct BlockingGC {
    std::atomic<int> calls{0};
    std::atomic<bool> release{false};
  } state;
  auto block_workers = +[](void* arg) {
    auto* state = reinterpret_cast<BlockingGC*>(arg);
    state->calls.fetch_add(1, std::memory_order_release);
    while (!state->release.load(std::memory_order_acquire)) {
      std::this_thread::yield();
    }
    return false;
  };
  TEST_SYNC_POINT_ARG("GC.BeforeBackground", &state);
  TEST_SYNC_POINT_CALLBACK("GC.BeforeBackground", block_workers);

  db->ManualGC(sources[0]);
  db->ManualGC(sources[1]);
  for (int i = 0;
       i < 5000 && state.calls.load(std::memory_order_acquire) < 2; ++i) {
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  EXPECT_EQ(2, state.calls.load(std::memory_order_acquire));

  std::atomic<bool> deleted{false};
  std::thread destroyer([&] {
    delete db;
    deleted.store(true, std::memory_order_release);
  });
  std::this_thread::sleep_for(std::chrono::milliseconds(50));
  EXPECT_FALSE(deleted.load(std::memory_order_acquire));
  state.release.store(true, std::memory_order_release);
  destroyer.join();
  EXPECT_TRUE(deleted.load(std::memory_order_acquire));

  TEST_SYNC_POINT_CLEAR("GC.BeforeBackground");
  CleanDir(options.env, dbname);
}

TEST(VLOG_TEST, DBWrapperManualGCQueuesDistinctFiles) {
  REQUIRE_TEST_SYNC_POINTS();
  ScopedGCSyncPoints sync_points;
  Options options;
  options.create_if_missing = true;
  options.blob_max_file_size = 32 << 10;
  options.blob_value_size_threshold = 256;
  options.blob_gc_size_discard_threshold = 100;
  options.blob_gc_num_discard_threshold = 100;
  options.blob_gc_interval = 0;
  const std::string dbname("testdb_gc_manual_queue");
  CleanDir(options.env, dbname);

  BlobDB* db = nullptr;
  ASSERT_TRUE(BlobDB::Open(options, dbname, &db).ok());
  const std::string value(8 << 10, 'v');
  for (int i = 0; i < 24; ++i) {
    ASSERT_TRUE(db->Put(WriteOptions(), "key" + std::to_string(i), value).ok());
  }

  std::vector<std::string> children;
  ASSERT_TRUE(options.env->GetChildren(dbname, &children).ok());
  std::set<uint64_t> file_numbers;
  for (const std::string& child : children) {
    uint64_t number = 0;
    FileType type;
    if (ParseFileName(child, &number, &type) && type == kVLogFile) {
      file_numbers.insert(number);
    }
  }
  // The largest vlog is the current writable file; choose three read-only
  // files so each request maps to a distinct eligible source.
  ASSERT_GE(file_numbers.size(), 4U);
  auto current = file_numbers.end();
  --current;
  file_numbers.erase(current);
  auto source = file_numbers.begin();
  const uint64_t first = *source++;
  const uint64_t second = *source++;
  const uint64_t third = *source++;

  struct BlockingGC {
    std::atomic<int> calls{0};
    std::atomic<bool> first_entered{false};
    std::atomic<bool> release_first{false};
  } state;
  auto block_first = +[](void* arg) {
    auto* state = reinterpret_cast<BlockingGC*>(arg);
    const int previous = state->calls.fetch_add(1, std::memory_order_relaxed);
    if (previous == 0) {
      state->first_entered.store(true, std::memory_order_release);
      while (!state->release_first.load(std::memory_order_acquire)) {
        std::this_thread::yield();
      }
    }
    return false;
  };
  TEST_SYNC_POINT_ARG("GC.BeforeBackground", &state);
  TEST_SYNC_POINT_CALLBACK("GC.BeforeBackground", block_first);

  db->ManualGC(first);
  for (int i = 0;
       i < 5000 && !state.first_entered.load(std::memory_order_acquire); ++i) {
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  EXPECT_TRUE(state.first_entered.load(std::memory_order_acquire));

  // Queue two requests while the first scheduled job is blocked. A single
  // boolean/file slot loses the first two requests by repeatedly overwriting
  // it; a coalescing set drains all three in separate jobs.
  db->ManualGC(second);
  db->ManualGC(third);
  std::this_thread::sleep_for(std::chrono::milliseconds(50));
  EXPECT_EQ(1, state.calls.load(std::memory_order_relaxed));
  state.release_first.store(true, std::memory_order_release);
  db->WaitVLogGC();

  EXPECT_EQ(3, state.calls.load(std::memory_order_relaxed));
  TEST_SYNC_POINT_CLEAR("GC.BeforeBackground");
  delete db;
  CleanDir(options.env, dbname);
}

TEST(VLOG_TEST, DBWrapperGCDefersUnpublishedRolledFile) {
  REQUIRE_TEST_SYNC_POINTS();
  ScopedGCSyncPoints sync_points;
  Options options;
  options.create_if_missing = true;
  options.blob_max_file_size = 4 << 10;
  options.blob_value_size_threshold = 1;
  options.blob_gc_size_discard_threshold = 100;
  options.blob_gc_num_discard_threshold = 100;
  options.blob_gc_interval = 0;
  const std::string dbname("testdb_gc_publication_pin");
  CleanDir(options.env, dbname);

  BlobDB* db = nullptr;
  ASSERT_TRUE(BlobDB::Open(options, dbname, &db).ok());

  class BlockingWriteCallback : public WriteCallback {
   public:
    Status Callback(DB*) override {
      entered.store(true, std::memory_order_release);
      while (!release.load(std::memory_order_acquire)) {
        std::this_thread::yield();
      }
      return Status::OK();
    }

    bool AllowGrouping() const override { return false; }

    std::atomic<bool> entered{false};
    std::atomic<bool> release{false};
  } callback;

  const std::string key("pinned-key");
  const std::string expected(8 << 10, 'p');
  WriteBatch batch;
  batch.Put(key, expected);
  Status write_status;
  std::thread writer([&] {
    write_status = db->Write(WriteOptions(), &batch, &callback);
  });

  for (int i = 0;
       i < 5000 && !callback.entered.load(std::memory_order_acquire); ++i) {
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  if (!callback.entered.load(std::memory_order_acquire)) {
    callback.release.store(true, std::memory_order_release);
    writer.join();
    ADD_FAILURE() << "write callback did not start";
    delete db;
    CleanDir(options.env, dbname);
    return;
  }

  // The oversized write has rolled its vlog, but its callback is deliberately
  // holding the LSM publication. The smaller file number is that read-only,
  // publication-pinned source; the larger one is the new writable vlog.
  std::vector<std::string> children;
  Status children_status = options.env->GetChildren(dbname, &children);
  if (!children_status.ok()) {
    callback.release.store(true, std::memory_order_release);
    writer.join();
    ADD_FAILURE() << children_status.ToString();
    delete db;
    CleanDir(options.env, dbname);
    return;
  }
  std::set<uint64_t> file_numbers;
  for (const std::string& child : children) {
    uint64_t number = 0;
    FileType type;
    if (ParseFileName(child, &number, &type) && type == kVLogFile) {
      file_numbers.insert(number);
    }
  }
  if (file_numbers.size() < 2U) {
    callback.release.store(true, std::memory_order_release);
    writer.join();
    ADD_FAILURE() << "oversized write did not roll its vlog";
    delete db;
    CleanDir(options.env, dbname);
    return;
  }
  const uint64_t source_number = *file_numbers.begin();
  const std::string source_name = VLogFileName(dbname, source_number);

  std::atomic<int> background_calls{0};
  auto count_call = +[](void* arg) {
    reinterpret_cast<std::atomic<int>*>(arg)->fetch_add(
        1, std::memory_order_relaxed);
    return false;
  };
  TEST_SYNC_POINT_ARG("GC.BeforeBackground", &background_calls);
  TEST_SYNC_POINT_CALLBACK("GC.BeforeBackground", count_call);

  Status gc_status = db->RunVLogGC();
  EXPECT_TRUE(gc_status.ok()) << gc_status.ToString();
  // A full pass filters pinned sources before scheduling background work.
  EXPECT_EQ(0, background_calls.load(std::memory_order_relaxed));
  EXPECT_TRUE(options.env->FileExists(source_name));

  // Publishing the handle releases the pin and automatically retries the
  // deferred request. The retry sees a live value and must retain the source.
  callback.release.store(true, std::memory_order_release);
  writer.join();
  EXPECT_TRUE(write_status.ok()) << write_status.ToString();
  db->WaitVLogGC();
  EXPECT_EQ(1, background_calls.load(std::memory_order_relaxed));
  EXPECT_TRUE(options.env->FileExists(source_name));

  std::string actual;
  Status get_status = db->Get(ReadOptions(), key, &actual);
  EXPECT_TRUE(get_status.ok()) << get_status.ToString();
  if (get_status.ok()) {
    EXPECT_EQ(expected, actual);
  }

  TEST_SYNC_POINT_CLEAR("GC.BeforeBackground");
  delete db;
  CleanDir(options.env, dbname);
}

TEST(VLOG_TEST, DBWrapperEmptyGCDoesNotBusyReschedule) {
  REQUIRE_TEST_SYNC_POINTS();
  ScopedGCSyncPoints sync_points;
  Options options;
  options.create_if_missing = true;
  options.blob_gc_interval = 1;
  const std::string dbname("testdb_gc_empty_schedule");
  CleanDir(options.env, dbname);

  std::atomic<int> background_calls{0};
  auto count_and_delay_first = +[](void* arg) {
    auto* calls = reinterpret_cast<std::atomic<int>*>(arg);
    const int previous = calls->fetch_add(1, std::memory_order_relaxed);
    if (previous == 0) {
      std::this_thread::sleep_for(std::chrono::milliseconds(1200));
    }
    return false;
  };
  TEST_SYNC_POINT_ARG("GC.BeforeBackground", &background_calls);
  TEST_SYNC_POINT_CALLBACK("GC.BeforeBackground", count_and_delay_first);

  BlobDB* db = nullptr;
  ASSERT_TRUE(BlobDB::Open(options, dbname, &db).ok());
  db->ManualGC(0);
  db->WaitVLogGC();
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  // The first empty scan takes longer than blob_gc_interval. The old scheduler
  // left gc_last_run_ unchanged and immediately requeued an unbounded stream
  // of empty scans when BGCall returned.
  ASSERT_EQ(1, background_calls.load(std::memory_order_relaxed));
  ASSERT_TRUE(db->VLogBGError().ok());

  delete db;
  TEST_SYNC_POINT_CLEAR("GC.BeforeBackground");
  CleanDir(options.env, dbname);
}

TEST(VLOG_TEST, DBWrapperPeriodicGCWithoutForegroundRollover) {
  REQUIRE_TEST_SYNC_POINTS();
  ScopedGCSyncPoints sync_points;
  Options options;
  options.create_if_missing = true;
  options.blob_gc_interval = 1;
  const std::string dbname("testdb_gc_periodic_idle");
  CleanDir(options.env, dbname);

  std::atomic<int> background_calls{0};
  auto count_call = +[](void* arg) {
    reinterpret_cast<std::atomic<int>*>(arg)->fetch_add(
        1, std::memory_order_relaxed);
    return false;
  };
  TEST_SYNC_POINT_ARG("GC.BeforeBackground", &background_calls);
  TEST_SYNC_POINT_CALLBACK("GC.BeforeBackground", count_call);

  BlobDB* db = nullptr;
  ASSERT_TRUE(BlobDB::Open(options, dbname, &db).ok());
  // No write, rollover, or manual GC call occurs after Open. The dedicated
  // timer must be the event which starts this empty maintenance pass.
  for (int i = 0;
       i < 500 && background_calls.load(std::memory_order_relaxed) == 0; ++i) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  ASSERT_GE(background_calls.load(std::memory_order_relaxed), 1);

  delete db;
  TEST_SYNC_POINT_CLEAR("GC.BeforeBackground");
  CleanDir(options.env, dbname);
}

TEST(VLOG_TEST, PeriodicGCFillsTwoWorkerEpoch) {
  REQUIRE_TEST_SYNC_POINTS();
  ScopedGCSyncPoints sync_points;
  Options options;
  options.create_if_missing = true;
  options.blob_max_file_size = 32 << 10;
  options.blob_value_size_threshold = 256;
  options.blob_gc_size_discard_threshold = 100;
  options.blob_gc_num_discard_threshold = 100;
  options.blob_gc_interval = 1;
  options.blob_max_background_gc = 2;
  const std::string dbname("testdb_gc_periodic_parallel_epoch");
  CleanDir(options.env, dbname);

  struct PeriodicWorkers {
    std::atomic<int> calls{0};
    std::atomic<int> active{0};
    std::atomic<int> max_active{0};
    std::atomic<bool> release{false};
  } state;
  auto block_periodic_workers = +[](void* arg) {
    auto* state = reinterpret_cast<PeriodicWorkers*>(arg);
    state->calls.fetch_add(1, std::memory_order_release);
    const int active =
        state->active.fetch_add(1, std::memory_order_acq_rel) + 1;
    int maximum = state->max_active.load(std::memory_order_relaxed);
    while (maximum < active &&
           !state->max_active.compare_exchange_weak(
               maximum, active, std::memory_order_relaxed)) {
    }
    while (!state->release.load(std::memory_order_acquire)) {
      std::this_thread::yield();
    }
    state->active.fetch_sub(1, std::memory_order_acq_rel);
    return false;
  };
  BlobDB* db = nullptr;
  ASSERT_TRUE(BlobDB::Open(options, dbname, &db).ok());
  const std::string value(8 << 10, 'v');
  for (int i = 0; i < 16; ++i) {
    ASSERT_TRUE(db->Put(WriteOptions(), "key" + std::to_string(i), value).ok());
  }
  ASSERT_GE(ReadOnlyVLogNumbers(options.env, dbname).size(), 2U);
  TEST_SYNC_POINT_ARG("GC.BeforeBackground", &state);
  TEST_SYNC_POINT_CALLBACK("GC.BeforeBackground", block_periodic_workers);
  for (int i = 0;
       i < 500 && state.calls.load(std::memory_order_acquire) < 2; ++i) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  EXPECT_EQ(2, state.calls.load(std::memory_order_acquire));
  EXPECT_EQ(2, state.max_active.load(std::memory_order_acquire));
  state.release.store(true, std::memory_order_release);
  db->WaitVLogGC();

  TEST_SYNC_POINT_CLEAR("GC.BeforeBackground");
  delete db;
  CleanDir(options.env, dbname);
}

TEST(VLOG_TEST, DBWrapperGCRemovesObsoleteFileAfterSnapshotRelease) {
  Options options;
  options.create_if_missing = true;
  options.blob_max_file_size = 64 << 10;
  options.blob_value_size_threshold = 256;
  options.blob_gc_size_discard_threshold = 1;
  options.blob_gc_num_discard_threshold = 1;
  options.blob_gc_interval = 1;
  const std::string dbname("testdb_gc_obsolete_lifecycle");
  CleanDir(options.env, dbname);

  BlobDB* db = nullptr;
  ASSERT_TRUE(BlobDB::Open(options, dbname, &db).ok());
  const std::string old_value(8 << 10, 'o');
  const std::string new_value(8 << 10, 'n');
  constexpr int kEntries = 12;
  for (int i = 0; i < kEntries; ++i) {
    ASSERT_TRUE(db->Put(WriteOptions(), "key" + std::to_string(i), old_value)
                    .ok());
  }

  std::vector<std::string> children;
  ASSERT_TRUE(options.env->GetChildren(dbname, &children).ok());
  uint64_t source_number = 0;
  for (const std::string& child : children) {
    uint64_t number = 0;
    FileType type;
    if (ParseFileName(child, &number, &type) && type == kVLogFile &&
        (source_number == 0 || number < source_number)) {
      source_number = number;
    }
  }
  ASSERT_NE(0, source_number);
  const std::string source_name = VLogFileName(dbname, source_number);

  const Snapshot* snapshot = db->GetSnapshot();
  for (int i = 0; i < kEntries; ++i) {
    ASSERT_TRUE(db->Put(WriteOptions(), "key" + std::to_string(i), new_value)
                    .ok());
  }

  db->ManualGC(source_number);
  db->WaitVLogGC();
  ASSERT_TRUE(db->VLogBGError().ok());
  ASSERT_TRUE(options.env->FileExists(source_name));

  ReadOptions snapshot_read;
  snapshot_read.snapshot = snapshot;
  std::string value;
  ASSERT_TRUE(db->Get(snapshot_read, "key0", &value).ok());
  ASSERT_EQ(old_value, value);

  db->ReleaseSnapshot(snapshot);
  // No foreground write or explicit RemoveObsoleteBlob() call follows the
  // release. A timer-driven maintenance pass must notice that the snapshot
  // barrier has expired and reclaim the old file.
  for (int i = 0; i < 500 && options.env->FileExists(source_name); ++i) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  ASSERT_FALSE(options.env->FileExists(source_name));

  for (int i = 0; i < kEntries; ++i) {
    ASSERT_TRUE(db->Get(ReadOptions(), "key" + std::to_string(i), &value).ok());
    ASSERT_EQ(new_value, value);
  }

  delete db;
  CleanDir(options.env, dbname);
}

TEST(VLOG_TEST, DBWrapperAutoGC) {
  Options options;
  Status s;
  options.env->NewStdLogger(&options.info_log);
  options.create_if_missing = true;
  options.blob_max_file_size = 8 << 20;
  options.blob_value_size_threshold = 256;
  options.blob_gc_interval = 2;
  std::string dbname("testdb");
  std::string value;
  CleanDir(options.env, dbname);
  int num_entries = 300000;

  TEST_SYNC_POINT_CLEAR("GC.AfterCollect");
  TEST_SYNC_POINT_CLEAR("GC.Rewrite.AfterValueRewrite");
  TEST_SYNC_POINT_CLEAR("GC.Rewrite.AfterLSMRewrite");

  BlobDB* db;
  BlobDB::Open(options, dbname, &db);

  std::unordered_map<std::string, std::string> kvmap;
  std::vector<std::string> rewrites;

  auto validate_fn = [&db, &kvmap, &value]() {
    Status s;
    for (const auto& p : kvmap) {
      std::string key = p.first;
      s = db->Get(ReadOptions(), key, &value);
      ASSERT_TRUE(s.ok());
      ASSERT_EQ(kvmap[key], value);
    }
  };

  size_t size = 0;
  for (int i = 0; i < num_entries; ++i) {
    std::string key = "key" + std::to_string(i);
    std::string val = "value" + std::string(256, 'x');
    kvmap[key] = val;
    if (size <= 3 * options.blob_max_file_size) {
      rewrites.emplace_back(key);
      size += SizeOf(key, val);
    }
    s = db->Put(WriteOptions(), key, val);
    ASSERT_TRUE(s.ok());
  }

  std::random_device rd;
  std::mt19937 g(rd());
  std::shuffle(rewrites.begin(), rewrites.end(), g);
  for (auto& key : rewrites) {
    std::string val = "NEWvalue" + std::string(256, 'x');
    s = db->Put(WriteOptions(), key, val);
    kvmap[key] = val;
    ASSERT_TRUE(s.ok());
  }

  std::shuffle(rewrites.begin(), rewrites.end(), g);
  std::this_thread::sleep_for(std::chrono::seconds(3));
  for (auto& key : rewrites) {
    std::string val = "NEWvalue2" + std::string(256, 'x');
    s = db->Put(WriteOptions(), key, val);
    ASSERT_TRUE(s.ok());
    kvmap[key] = val;
  }

  delete db;
  BlobDB::Open(options, dbname, &db);

  std::shuffle(rewrites.begin(), rewrites.end(), g);
  std::this_thread::sleep_for(std::chrono::seconds(3));
  for (auto& key : rewrites) {
    std::string val = "NEWvalue3" + std::string(256, 'x');
    s = db->Put(WriteOptions(), key, val);
    ASSERT_TRUE(s.ok());
    kvmap[key] = val;
  }

  db->WaitVLogGC();

  delete db;
  BlobDB::Open(options, dbname, &db);

  db->RemoveObsoleteBlob();

  validate_fn();

  printf("%s", reinterpret_cast<BlobDB*>(db)->DebugString().c_str());
  delete db;
  CleanDir(options.env, dbname);
}

TEST(VLOG_TEST, DBWrapperGCAsync) {
  ScopedGCSyncPoints sync_points;
  Options options;
  Status s;
  options.env->NewStdLogger(&options.info_log);
  options.create_if_missing = true;
  options.blob_max_file_size = 8 << 20;
  options.blob_value_size_threshold = 256;
  std::string dbname("testdb");
  std::string value;
  port::RWMutex rwlock;
  CleanDir(options.env, dbname);
  int num_entries = 100000;

  BlobDB* db;
  BlobDB::Open(options, dbname, &db);

  std::unordered_map<std::string, std::string> kvmap;
  std::vector<std::string> rewrites;

  auto validate_fn = [&db, &kvmap, &value]() {
    Status s;
    for (const auto& p : kvmap) {
      std::string key = p.first;
      s = db->Get(ReadOptions(), key, &value);
      ASSERT_TRUE(s.ok());
      ASSERT_EQ(kvmap[key], value);
    }
  };

  size_t size = 0;
  for (int i = 0; i < num_entries; ++i) {
    std::string key = "key" + std::to_string(i);
    std::string val = "value" + std::string(256, 'x');
    kvmap[key] = val;
    if (size <= options.blob_max_file_size) {
      rewrites.emplace_back(key);
      size += SizeOf(key, val);
    }
    s = db->Put(WriteOptions(), key, val);
    ASSERT_TRUE(s.ok());
  }

  std::random_device rd;
  std::mt19937 g(rd());
  std::shuffle(rewrites.begin(), rewrites.end(), g);

  int d = 0;
  for (auto& key : rewrites) {
    std::string val = "NEWvalue" + std::string(256, 'x');
    s = db->Put(WriteOptions(), key, val);
    ASSERT_TRUE(s.ok());

    kvmap[key] = val;
    d++;
    if (d > rewrites.size() / 2 + 1) {
      break;
    }
  }

  std::shuffle(rewrites.begin(), rewrites.end(), g);

  struct sync_validate_arg {
    BlobDB* db;
    port::RWMutex* lock;
    std::unordered_map<std::string, std::string>* kvs;
  };
  sync_validate_arg validateArg = {.db = db, .lock = &rwlock, .kvs = &kvmap};
  auto validate_cb = +[](void* arg) {
    BlobDB* db = reinterpret_cast<sync_validate_arg*>(arg)->db;
    auto* kvmap = reinterpret_cast<sync_validate_arg*>(arg)->kvs;
    auto* lk = reinterpret_cast<sync_validate_arg*>(arg)->lock;
    std::string value;
    Status s;
    lk->RLock();
    for (const auto& p : *kvmap) {
      std::string key = p.first;
      s = db->Get(ReadOptions(), key, &value);
      assert(s.ok());
      assert((*kvmap)[key] == value);
    }
    lk->RUnlock();
    return false;
  };

  TEST_SYNC_POINT_CLEAR("GC.AfterCollect");

  TEST_SYNC_POINT_ARG("GC.Rewrite.AfterValueRewrite", &validateArg);
  TEST_SYNC_POINT_CALLBACK("GC.Rewrite.AfterValueRewrite", validate_cb);

  TEST_SYNC_POINT_ARG("GC.Rewrite.AfterLSMRewrite", &validateArg);
  TEST_SYNC_POINT_CALLBACK("GC.Rewrite.AfterLSMRewrite", validate_cb);

  db->ManualGC(0);  // discard ratio ~50%

  for (auto& key : rewrites) {
    std::string val = "NEWvalue2" + std::string(256, 'x');
    WriteLock l(&rwlock);
    s = db->Put(WriteOptions(), key, val);
    ASSERT_TRUE(s.ok());
    kvmap[key] = val;
  }

  db->WaitVLogGC();

  delete db;
  BlobDB::Open(options, dbname, &db);

  // Put one more record to expire the old vlog file
  db->Put(WriteOptions(), "OneMoreKey", "value");
  db->RemoveObsoleteBlob();

  delete db;
  BlobDB::Open(options, dbname, &db);

  validate_fn();

  printf("%s", reinterpret_cast<BlobDB*>(db)->DebugString().c_str());
  delete db;
  CleanDir(options.env, dbname);
}

TEST(VLOG_TEST, DBWrapperGCOverwriteAfterCollect) {
  ScopedGCSyncPoints sync_points;
  Options options;
  Status s;
  options.env->NewStdLogger(&options.info_log);
  options.create_if_missing = true;
  options.blob_max_file_size = 8 << 20;
  options.blob_value_size_threshold = 256;
  std::string dbname("testdb");
  std::string value;
  CleanDir(options.env, dbname);
  int num_entries = 100000;

  BlobDB* db;
  BlobDB::Open(options, dbname, &db);

  std::unordered_map<std::string, std::string> kvmap;
  std::vector<std::string> rewrites;

  auto validate_fn = [&db, &kvmap, &value]() {
    Status s;
    for (const auto& p : kvmap) {
      std::string key = p.first;
      s = db->Get(ReadOptions(), key, &value);
      ASSERT_TRUE(s.ok());
      ASSERT_EQ(kvmap[key], value);
    }
  };

  size_t size = 0;
  for (int i = 0; i < num_entries; ++i) {
    std::string key = "key" + std::to_string(i);
    std::string val = "value" + std::string(256, 'x');
    kvmap[key] = val;
    if (size <= options.blob_max_file_size) {
      rewrites.emplace_back(key);
      size += SizeOf(key, val);
    }
    s = db->Put(WriteOptions(), key, val);
    ASSERT_TRUE(s.ok());
  }

  std::random_device rd;
  std::mt19937 g(rd());
  std::shuffle(rewrites.begin(), rewrites.end(), g);

  int d = 0;
  for (auto& key : rewrites) {
    std::string val = "NEWvalue" + std::string(256, 'x');
    s = db->Put(WriteOptions(), key, val);
    ASSERT_TRUE(s.ok());

    kvmap[key] = val;
    d++;
    if (d > rewrites.size() / 2 + 1) {
      break;
    }
  }

  std::shuffle(rewrites.begin(), rewrites.end(), g);

  struct sync_validate_arg {
    BlobDB* db;
    std::unordered_map<std::string, std::string>* kvs;
  };
  sync_validate_arg validateArg = {.db = db, .kvs = &kvmap};
  auto validate_cb = +[](void* arg) {
    BlobDB* db = reinterpret_cast<sync_validate_arg*>(arg)->db;
    auto* kvmap = reinterpret_cast<sync_validate_arg*>(arg)->kvs;
    std::string value;
    Status s;
    for (const auto& p : *kvmap) {
      std::string key = p.first;
      s = db->Get(ReadOptions(), key, &value);
      assert(s.ok());
      assert((*kvmap)[key] == value);
    }
    return false;
  };

  struct sync_overwrite_arg {
    BlobDB* db;
    std::unordered_map<std::string, std::string>* kvs;
    std::vector<std::string>* rewrites;
  };
  sync_overwrite_arg overwriteArg = {
      .db = db, .kvs = &kvmap, .rewrites = &rewrites};
  auto overwrite_cb = +[](void* arg) {
    BlobDB* db = reinterpret_cast<sync_overwrite_arg*>(arg)->db;
    auto* rewrites = reinterpret_cast<sync_overwrite_arg*>(arg)->rewrites;
    auto* kvmap = reinterpret_cast<sync_overwrite_arg*>(arg)->kvs;
    std::string value;
    Status s;
    for (const auto& key : *rewrites) {
      std::string val =
          "NEWvalue2" + ((rand() % 2 == 0) ? "" : std::string(256, 'x'));
      s = db->Put(WriteOptions(), key, val);
      (*kvmap)[key] = val;
      assert(s.ok());
    }
    return false;
  };

  TEST_SYNC_POINT_ARG("GC.Rewrite.AfterValueRewrite", &validateArg);
  TEST_SYNC_POINT_CALLBACK("GC.Rewrite.AfterValueRewrite", validate_cb);

  TEST_SYNC_POINT_ARG("GC.Rewrite.AfterLSMRewrite", &validateArg);
  TEST_SYNC_POINT_CALLBACK("GC.Rewrite.AfterLSMRewrite", validate_cb);

  TEST_SYNC_POINT_ARG("GC.AfterCollect", &overwriteArg);
  TEST_SYNC_POINT_CALLBACK("GC.AfterCollect", overwrite_cb);

  db->ManualGC(0);  // discard ratio ~50%

  db->WaitVLogGC();

  delete db;
  BlobDB::Open(options, dbname, &db);

  // Put one more record to expire the old vlog file
  db->Put(WriteOptions(), "OneMoreKey", "value");
  db->RemoveObsoleteBlob();

  delete db;
  BlobDB::Open(options, dbname, &db);

  validate_fn();

  printf("%s", reinterpret_cast<BlobDB*>(db)->DebugString().c_str());
  delete db;
  CleanDir(options.env, dbname);
}

TEST(VLOG_TEST, DBWrapperManualGCSync) {
  ScopedGCSyncPoints sync_points;
  Options options;
  Status s;
  options.env->NewStdLogger(&options.info_log);
  options.create_if_missing = true;
  options.blob_max_file_size = 8 << 20;
  options.blob_value_size_threshold = 256;
  std::string dbname("testdb");
  std::string value;
  CleanDir(options.env, dbname);
  int num_entries = 100000;

  BlobDB* db;
  BlobDB::Open(options, dbname, &db);

  std::unordered_map<std::string, std::string> kvmap;
  std::vector<std::string> rewrites;

  auto validate_fn = [&db, &kvmap, &value]() {
    Status s;
    for (const auto& p : kvmap) {
      std::string key = p.first;
      s = db->Get(ReadOptions(), key, &value);
      ASSERT_TRUE(s.ok());
      ASSERT_EQ(kvmap[key], value);
    }
  };

  size_t size = 0;
  for (int i = 0; i < num_entries; ++i) {
    std::string key = "key" + std::to_string(i);
    std::string val = "value" + std::string(256, 'x');
    kvmap[key] = val;
    if (size <= options.blob_max_file_size) {
      rewrites.emplace_back(key);
      size += SizeOf(key, val);
    }
    s = db->Put(WriteOptions(), key, val);
    ASSERT_TRUE(s.ok());
  }

  db->ManualGC(0);  // discard ratio 0%
  db->WaitVLogGC();
  ASSERT_TRUE(db->VLogBGError().IsNonFatal());

  std::random_device rd;
  std::mt19937 g(rd());
  std::shuffle(rewrites.begin(), rewrites.end(), g);

  int d = 0;
  for (auto& key : rewrites) {
    std::string val = "NEWvalue" + std::string(256, 'x');
    s = db->Put(WriteOptions(), key, val);
    ASSERT_TRUE(s.ok());

    kvmap[key] = val;
    d++;
    if (d > rewrites.size() / 2 + 1) {
      break;
    }
  }

  struct sync_point_arg {
    BlobDB* db;
    std::unordered_map<std::string, std::string>* kvs;
  };
  sync_point_arg myarg = {.db = db, .kvs = &kvmap};
  auto sync_point_cb = +[](void* arg) {
    BlobDB* db = reinterpret_cast<sync_point_arg*>(arg)->db;
    auto* kvmap = reinterpret_cast<sync_point_arg*>(arg)->kvs;
    std::string value;
    Status s;
    for (const auto& p : *kvmap) {
      std::string key = p.first;
      s = db->Get(ReadOptions(), key, &value);
      assert(s.ok());
      assert((*kvmap)[key] == value);
    }
    return false;
  };

  TEST_SYNC_POINT_CLEAR("GC.AfterCollect");

  TEST_SYNC_POINT_ARG("GC.Rewrite.AfterValueRewrite", &myarg);
  TEST_SYNC_POINT_CALLBACK("GC.Rewrite.AfterValueRewrite", sync_point_cb);

  TEST_SYNC_POINT_ARG("GC.Rewrite.AfterLSMRewrite", &myarg);
  TEST_SYNC_POINT_CALLBACK("GC.Rewrite.AfterLSMRewrite", sync_point_cb);

  db->ManualGC(0);  // discard ratio ~50%
  db->WaitVLogGC();
  ASSERT_TRUE(db->VLogBGError().ok());

  delete db;
  BlobDB::Open(options, dbname, &db);

  // Put one more record to expire the old vlog file
  db->Put(WriteOptions(), "OneMoreKey", "value");
  db->RemoveObsoleteBlob();

  delete db;
  BlobDB::Open(options, dbname, &db);

  validate_fn();

  printf("%s", reinterpret_cast<BlobDB*>(db)->DebugString().c_str());
  delete db;
  CleanDir(options.env, dbname);
}

namespace {

// Blocks one WAL append before it reaches the underlying WritableFile. This
// makes it possible to verify that DBImpl::Sync joins the writer queue instead
// of concurrently invoking Sync on a single-threaded WritableFile.
class BlockingLogEnv : public EnvWrapper {
 public:
  explicit BlockingLogEnv(Env* target) : EnvWrapper(target) {}

  Status NewWritableFile(const std::string& filename,
                         WritableFile** result) override {
    WritableFile* target_file = nullptr;
    Status s = target()->NewWritableFile(filename, &target_file);
    if (!s.ok()) {
      *result = nullptr;
      return s;
    }

    const std::string::size_type separator = filename.rfind('/');
    const std::string::size_type basename =
        separator == std::string::npos ? 0 : separator + 1;
    constexpr char kVLogManifestPrefix[] = "VLOG-MANIFEST-";
    if (filename.compare(basename, sizeof(kVLogManifestPrefix) - 1,
                         kVLogManifestPrefix) == 0) {
      *result = new ManifestFile(this, target_file);
    } else if (filename.size() >= 4 &&
        filename.compare(filename.size() - 4, 4, ".log") == 0) {
      log_file_creations_.fetch_add(1, std::memory_order_relaxed);
      *result = new LogFile(this, target_file);
    } else if (filename.size() >= 4 &&
               filename.compare(filename.size() - 4, 4, ".ldb") == 0) {
      *result = new TableFile(this, target_file);
    } else {
      *result = target_file;
    }
    return Status::OK();
  }

  Status NewAppendableRandomAccessFile(
      const std::string& filename,
      AppendableRandomAccessFile** result) override {
    AppendableRandomAccessFile* target_file = nullptr;
    Status s =
        target()->NewAppendableRandomAccessFile(filename, &target_file);
    if (!s.ok()) {
      *result = nullptr;
      return s;
    }
    *result = new VLogFile(this, target_file);
    return Status::OK();
  }

  void BlockNextLogAppend() {
    append_blocked_.store(false, std::memory_order_release);
    release_append_.store(false, std::memory_order_release);
    sync_during_append_.store(false, std::memory_order_release);
    block_next_append_.store(true, std::memory_order_release);
  }

  void WaitForBlockedLogAppend() {
    while (!append_blocked_.load(std::memory_order_acquire)) {
      target()->SleepForMicroseconds(100);
    }
  }

  void ReleaseLogAppend() {
    release_append_.store(true, std::memory_order_release);
  }

  bool SyncDuringBlockedAppend() const {
    return sync_during_append_.load(std::memory_order_acquire);
  }

  void FailNextLogSync() {
    fail_next_sync_.store(true, std::memory_order_release);
  }

  void FailNextVLogSync() {
    fail_next_vlog_sync_.store(true, std::memory_order_release);
  }

  void FailLogAppendAfterSuccessOnCall(int call) {
    fail_append_as_invalid_argument_.store(false, std::memory_order_release);
    fail_append_countdown_.store(call, std::memory_order_release);
  }

  void FailLogAppendAfterSuccessWithInvalidArgumentOnCall(int call) {
    fail_append_as_invalid_argument_.store(true, std::memory_order_release);
    fail_append_countdown_.store(call, std::memory_order_release);
  }

  void FailNextVLogManifestSyncAfterSuccess() {
    fail_next_manifest_sync_.store(true, std::memory_order_release);
  }

  void BlockTableSync() {
    table_sync_blocked_.store(false, std::memory_order_release);
    release_table_sync_.store(false, std::memory_order_release);
    block_table_sync_.store(true, std::memory_order_release);
  }

  void WaitForBlockedTableSync() {
    while (!table_sync_blocked_.load(std::memory_order_acquire)) {
      target()->SleepForMicroseconds(100);
    }
  }

  void ReleaseTableSync() {
    release_table_sync_.store(true, std::memory_order_release);
  }

  int LogFileCreations() const {
    return log_file_creations_.load(std::memory_order_acquire);
  }

  bool ClosedDirtyLog() const {
    return closed_dirty_log_.load(std::memory_order_acquire);
  }

 private:
  class VLogFile : public AppendableRandomAccessFile {
   public:
    VLogFile(BlockingLogEnv* env, AppendableRandomAccessFile* target)
        : env_(env), target_(target) {}
    ~VLogFile() override { delete target_; }

    Status Read(uint64_t offset, size_t n, Slice* result,
                char* scratch) const override {
      return target_->Read(offset, n, result, scratch);
    }
    Status Append(const Slice& data) override { return target_->Append(data); }
    Status AppendV(const Slice* parts, size_t count) override {
      return target_->AppendV(parts, count);
    }
    Status WriteAtV(uint64_t offset, const Slice* parts,
                    size_t count) override {
      return target_->WriteAtV(offset, parts, count);
    }
    Status Truncate(uint64_t size) override {
      return target_->Truncate(size);
    }
    Status Close() override { return target_->Close(); }
    Status Flush() override { return target_->Flush(); }
    Status Sync() override {
      if (env_->fail_next_vlog_sync_.exchange(false,
                                               std::memory_order_acq_rel)) {
        return Status::IOError("injected vlog sync error");
      }
      return target_->Sync();
    }
    uint64_t Offset() override { return target_->Offset(); }

   private:
    BlockingLogEnv* const env_;
    AppendableRandomAccessFile* const target_;
  };

  class ManifestFile : public WritableFile {
   public:
    ManifestFile(BlockingLogEnv* env, WritableFile* target)
        : env_(env), target_(target) {}
    ~ManifestFile() override { delete target_; }

    Status Append(const Slice& data) override { return target_->Append(data); }
    Status Close() override { return target_->Close(); }
    Status Flush() override { return target_->Flush(); }
    Status Sync() override {
      Status s = target_->Sync();
      if (s.ok() && env_->fail_next_manifest_sync_.exchange(
                        false, std::memory_order_acq_rel)) {
        return Status::IOError(
            "injected vlog manifest sync error after persistence");
      }
      return s;
    }

   private:
    BlockingLogEnv* const env_;
    WritableFile* const target_;
  };

  class LogFile : public WritableFile {
   public:
    LogFile(BlockingLogEnv* env, WritableFile* target)
        : env_(env), target_(target) {}
    ~LogFile() override { delete target_; }

    Status Append(const Slice& data) override {
      if (env_->block_next_append_.exchange(false,
                                             std::memory_order_acq_rel)) {
        env_->append_blocked_.store(true, std::memory_order_release);
        while (!env_->release_append_.load(std::memory_order_acquire)) {
          env_->target()->SleepForMicroseconds(100);
        }
        env_->append_blocked_.store(false, std::memory_order_release);
      }
      Status s = target_->Append(data);
      if (s.ok()) {
        dirty_.store(true, std::memory_order_release);
        const int countdown =
            env_->fail_append_countdown_.load(std::memory_order_acquire);
        if (countdown > 0 &&
            env_->fail_append_countdown_.fetch_sub(
                1, std::memory_order_acq_rel) == 1) {
          Status flushed = target_->Flush();
          if (!flushed.ok()) return flushed;
          if (env_->fail_append_as_invalid_argument_.exchange(
                  false, std::memory_order_acq_rel)) {
            return Status::InvalidArgument(
                "injected non-IO WAL append error after write");
          }
          return Status::IOError("injected WAL append error after write");
        }
      }
      return s;
    }

    Status Close() override {
      if (dirty_.load(std::memory_order_acquire)) {
        env_->closed_dirty_log_.store(true, std::memory_order_release);
      }
      return target_->Close();
    }
    Status Flush() override { return target_->Flush(); }

    Status Sync() override {
      if (env_->append_blocked_.load(std::memory_order_acquire)) {
        env_->sync_during_append_.store(true, std::memory_order_release);
        // Do not invoke the underlying single-threaded file concurrently.
        return Status::OK();
      }
      if (env_->fail_next_sync_.exchange(false,
                                         std::memory_order_acq_rel)) {
        Status persisted = target_->Sync();
        if (!persisted.ok()) return persisted;
        dirty_.store(false, std::memory_order_release);
        return Status::IOError("injected WAL sync error after persistence");
      }
      Status s = target_->Sync();
      if (s.ok()) {
        dirty_.store(false, std::memory_order_release);
      }
      return s;
    }

   private:
    BlockingLogEnv* const env_;
    WritableFile* const target_;
    std::atomic<bool> dirty_{false};
  };

  class TableFile : public WritableFile {
   public:
    TableFile(BlockingLogEnv* env, WritableFile* target)
        : env_(env), target_(target) {}
    ~TableFile() override { delete target_; }

    Status Append(const Slice& data) override { return target_->Append(data); }
    Status Close() override { return target_->Close(); }
    Status Flush() override { return target_->Flush(); }
    Status Sync() override {
      if (env_->block_table_sync_.exchange(false,
                                            std::memory_order_acq_rel)) {
        env_->table_sync_blocked_.store(true, std::memory_order_release);
        while (!env_->release_table_sync_.load(std::memory_order_acquire)) {
          env_->target()->SleepForMicroseconds(100);
        }
        env_->table_sync_blocked_.store(false, std::memory_order_release);
      }
      return target_->Sync();
    }

   private:
    BlockingLogEnv* const env_;
    WritableFile* const target_;
  };

  std::atomic<bool> block_next_append_{false};
  std::atomic<bool> append_blocked_{false};
  std::atomic<bool> release_append_{false};
  std::atomic<bool> sync_during_append_{false};
  std::atomic<int> log_file_creations_{0};
  std::atomic<bool> closed_dirty_log_{false};
  std::atomic<bool> fail_next_sync_{false};
  std::atomic<bool> fail_next_vlog_sync_{false};
  std::atomic<bool> fail_next_manifest_sync_{false};
  std::atomic<int> fail_append_countdown_{0};
  std::atomic<bool> fail_append_as_invalid_argument_{false};
  std::atomic<bool> block_table_sync_{false};
  std::atomic<bool> table_sync_blocked_{false};
  std::atomic<bool> release_table_sync_{false};
};

void WaitForWriterQueueSize(DBImpl* db, size_t expected) {
  while (db->TEST_WriterQueueSize() < expected) {
    Env::Default()->SleepForMicroseconds(100);
  }
}

}  // namespace

TEST(VLOG_TEST, ParallelGCFaultStagesDoNotStrandOtherWorker) {
  REQUIRE_TEST_SYNC_POINTS();
  ScopedGCSyncPoints sync_points;
  enum class FailureStage { kReplacementSync, kLSMSync, kManifestSync };
  const std::vector<std::pair<FailureStage, std::string>> stages = {
      {FailureStage::kReplacementSync, "replacement"},
      {FailureStage::kLSMSync, "lsm"},
      {FailureStage::kManifestSync, "manifest"},
  };

  for (const auto& stage : stages) {
    SCOPED_TRACE(stage.second);
    BlockingLogEnv env(Env::Default());
    Options options;
    options.env = &env;
    options.create_if_missing = true;
    options.blob_max_file_size = 32 << 10;
    options.blob_value_size_threshold = 256;
    options.blob_gc_size_discard_threshold = 0;
    options.blob_gc_num_discard_threshold = 0;
    options.blob_gc_interval = 0;
    options.blob_max_background_gc = 2;
    const std::string dbname =
        "testdb_gc_parallel_" + stage.second + "_failure";
    CleanDir(options.env, dbname);

    BlobDB* db = nullptr;
    ASSERT_TRUE(BlobDB::Open(options, dbname, &db).ok());
    const std::string value(8 << 10, 'v');
    for (int i = 0; i < 16; ++i) {
      ASSERT_TRUE(
          db->Put(WriteOptions(), "key" + std::to_string(i), value).ok());
    }
    const std::vector<uint64_t> sources =
        ReadOnlyVLogNumbers(options.env, dbname);
    ASSERT_GE(sources.size(), 2U);

    struct SyncFailure {
      BlockingLogEnv* env;
      FailureStage stage;
      std::atomic<int> collected{0};
      std::atomic<int> lsm_synced{0};
      std::atomic<bool> release_second{false};
    } failure;
    failure.env = &env;
    failure.stage = stage.first;
    auto coordinate_collect = +[](void* arg) {
      auto* failure = reinterpret_cast<SyncFailure*>(arg);
      const int collected =
          failure->collected.fetch_add(1, std::memory_order_acq_rel) + 1;
      if (failure->stage == FailureStage::kReplacementSync) {
        if (collected == 2) {
          failure->env->FailNextVLogSync();
          failure->release_second.store(true, std::memory_order_release);
        }
        while (!failure->release_second.load(std::memory_order_acquire)) {
          std::this_thread::yield();
        }
      } else if (collected == 2) {
        while (!failure->release_second.load(std::memory_order_acquire)) {
          std::this_thread::yield();
        }
      }
      return false;
    };
    auto inject_later_failure = +[](void* arg) {
      auto* failure = reinterpret_cast<SyncFailure*>(arg);
      const int synced =
          failure->lsm_synced.fetch_add(1, std::memory_order_acq_rel) + 1;
      if (synced == 1) {
        if (failure->stage == FailureStage::kLSMSync) {
          failure->env->FailNextLogSync();
        } else if (failure->stage == FailureStage::kManifestSync) {
          failure->env->FailNextVLogManifestSyncAfterSuccess();
        }
        failure->release_second.store(true, std::memory_order_release);
      }
      return false;
    };
    TEST_SYNC_POINT_ARG("GC.AfterCollect", &failure);
    TEST_SYNC_POINT_CALLBACK("GC.AfterCollect", coordinate_collect);
    TEST_SYNC_POINT_ARG("GC.Rewrite.AfterLSMRewrite", &failure);
    TEST_SYNC_POINT_CALLBACK("GC.Rewrite.AfterLSMRewrite",
                             inject_later_failure);

    db->ManualGC(sources[0]);
    db->ManualGC(sources[1]);
    db->WaitVLogGC();
    EXPECT_EQ(2, failure.collected.load(std::memory_order_acquire));
    const int expected_lsm_synced =
        stage.first == FailureStage::kManifestSync ? 2 : 1;
    EXPECT_EQ(expected_lsm_synced,
              failure.lsm_synced.load(std::memory_order_acquire));
    EXPECT_TRUE(db->VLogBGError().IsIOError())
        << db->VLogBGError().ToString();

    TEST_SYNC_POINT_CLEAR("GC.AfterCollect");
    TEST_SYNC_POINT_CLEAR("GC.Rewrite.AfterLSMRewrite");
    delete db;
    db = nullptr;
    ASSERT_TRUE(BlobDB::Open(options, dbname, &db).ok());
    std::string read;
    for (int i = 0; i < 16; ++i) {
      ASSERT_TRUE(
          db->Get(ReadOptions(), "key" + std::to_string(i), &read).ok());
      EXPECT_EQ(value, read);
    }

    delete db;
    CleanDir(options.env, dbname);
  }
}

TEST(DBIMPL_TEST, WriteCallback) {
  Options options;
  Status s;
  options.env->NewStdLogger(&options.info_log);
  options.create_if_missing = true;
  std::string dbname("testdb");
  std::string value;
  CleanDir(options.env, dbname);

  DB* db;
  DB::Open(options, dbname, &db);

  class TestWriteCallback2 : public WriteCallback {
   public:
    TestWriteCallback2(std::string&& key) : key_(key) {}
    ~TestWriteCallback2() override = default;
    Status Callback(DB* db) override {
      std::string value;
      return db->Get(ReadOptions(), key_, &value);
    }
    bool AllowGrouping() const override { return true; }
    std::string key_;
  };

  TestWriteCallback2 cb("key0");
  WriteBatch wb;

  wb.Put("key1", "val1");
  s = db->Write(WriteOptions(), &wb, &cb);
  ASSERT_TRUE(s.IsNotFound());

  s = db->Get(ReadOptions(), "key1", &value);
  ASSERT_TRUE(s.IsNotFound());

  db->Put(WriteOptions(), "key0", "val0");
  s = db->Write(WriteOptions(), &wb, &cb);
  ASSERT_TRUE(s.ok());
  s = db->Get(ReadOptions(), "key1", &value);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(value, "val1");

  delete db;
  CleanDir(options.env, dbname);
}

TEST(DBIMPL_TEST, BuildWriterGroup) {
  Options options;
  Status s;
  options.env->NewStdLogger(&options.info_log);
  options.create_if_missing = true;
  std::string dbname("testdb");
  std::string value;
  CleanDir(options.env, dbname);

  DBImpl* dbImpl;
  DB* db;
  DB::Open(options, dbname, &db);
  dbImpl = reinterpret_cast<DBImpl*>(db);

  dbImpl->TEST_BuildWriterGroup();

  delete db;
  CleanDir(options.env, dbname);
}

TEST(DBIMPL_TEST, SyncWaitsForActiveLogAppend) {
  BlockingLogEnv env(Env::Default());
  Options options;
  options.env = &env;
  options.create_if_missing = true;
  const std::string dbname("testdb-dbimpl-sync-barrier");
  CleanDir(options.env, dbname);

  DB* db = nullptr;
  ASSERT_TRUE(DB::Open(options, dbname, &db).ok());
  ASSERT_NE(nullptr, db);
  DBImpl* db_impl = static_cast<DBImpl*>(db);

  env.BlockNextLogAppend();
  Status write_status;
  Status sync_status;
  std::atomic<bool> sync_started{false};
  std::atomic<bool> sync_done{false};

  std::thread writer([&] {
    write_status = db->Put(WriteOptions(), "key", "value");
  });
  env.WaitForBlockedLogAppend();

  std::thread syncer([&] {
    sync_started.store(true, std::memory_order_release);
    sync_status = db_impl->Sync();
    sync_done.store(true, std::memory_order_release);
  });
  while (!sync_started.load(std::memory_order_acquire)) {
    options.env->SleepForMicroseconds(100);
  }

  size_t queue_size = db_impl->TEST_WriterQueueSize();
  while (queue_size < 2 && !sync_done.load(std::memory_order_acquire)) {
    options.env->SleepForMicroseconds(100);
    queue_size = db_impl->TEST_WriterQueueSize();
  }

  EXPECT_GE(queue_size, 2U);
  EXPECT_FALSE(sync_done.load(std::memory_order_acquire));
  EXPECT_FALSE(env.SyncDuringBlockedAppend());

  env.ReleaseLogAppend();
  writer.join();
  syncer.join();

  EXPECT_TRUE(write_status.ok()) << write_status.ToString();
  EXPECT_TRUE(sync_status.ok()) << sync_status.ToString();
  EXPECT_FALSE(env.SyncDuringBlockedAppend());

  delete db;
  CleanDir(options.env, dbname);
}

TEST(DBIMPL_TEST, OrdinaryWriterWaitsForOrderedLogAppend) {
  BlockingLogEnv env(Env::Default());
  Options options;
  options.env = &env;
  options.create_if_missing = true;
  const std::string dbname("testdb-dbimpl-ordered-write-barrier");
  CleanDir(options.env, dbname);

  DB* db = nullptr;
  ASSERT_TRUE(DB::Open(options, dbname, &db).ok());
  ASSERT_NE(nullptr, db);
  DBImpl* db_impl = static_cast<DBImpl*>(db);

  WriteBatch ordered_batch;
  ordered_batch.Put("ordered", "first");
  DBImpl::WriteState ordered_state = DBImpl::WriteState::kNotWritten;
  Status ordered_status;
  Status ordinary_status;
  std::atomic<bool> ordinary_done{false};

  env.BlockNextLogAppend();
  std::thread ordered_writer([&] {
    ordered_status = db_impl->WriteOrderedWithOutcome(
        WriteOptions(), &ordered_batch, nullptr, &ordered_state);
  });
  env.WaitForBlockedLogAppend();

  std::thread ordinary_writer([&] {
    ordinary_status = db->Put(WriteOptions(), "ordinary", "second");
    ordinary_done.store(true, std::memory_order_release);
  });
  WaitForWriterQueueSize(db_impl, 1);
  EXPECT_FALSE(ordinary_done.load(std::memory_order_acquire));

  env.ReleaseLogAppend();
  ordered_writer.join();
  ordinary_writer.join();

  EXPECT_TRUE(ordered_status.ok()) << ordered_status.ToString();
  EXPECT_EQ(DBImpl::WriteState::kApplied, ordered_state);
  EXPECT_TRUE(ordinary_status.ok()) << ordinary_status.ToString();
  EXPECT_EQ(2U, db_impl->LatestSequence());

  std::string value;
  ASSERT_TRUE(db->Get(ReadOptions(), "ordered", &value).ok());
  EXPECT_EQ("first", value);
  ASSERT_TRUE(db->Get(ReadOptions(), "ordinary", &value).ok());
  EXPECT_EQ("second", value);

  delete db;
  CleanDir(options.env, dbname);
}

TEST(DBIMPL_TEST, OrderedWriterWaitsForOrdinaryLogAppend) {
  BlockingLogEnv env(Env::Default());
  Options options;
  options.env = &env;
  options.create_if_missing = true;
  const std::string dbname("testdb-dbimpl-ordinary-write-barrier");
  CleanDir(options.env, dbname);

  DB* db = nullptr;
  ASSERT_TRUE(DB::Open(options, dbname, &db).ok());
  ASSERT_NE(nullptr, db);
  DBImpl* db_impl = static_cast<DBImpl*>(db);

  Status ordinary_status;
  env.BlockNextLogAppend();
  std::thread ordinary_writer([&] {
    ordinary_status = db->Put(WriteOptions(), "ordinary", "first");
  });
  env.WaitForBlockedLogAppend();

  WriteBatch ordered_batch;
  ordered_batch.Put("ordered", "second");
  DBImpl::WriteState ordered_state = DBImpl::WriteState::kNotWritten;
  Status ordered_status;
  std::atomic<bool> ordered_started{false};
  std::atomic<bool> ordered_done{false};
  std::thread ordered_writer([&] {
    ordered_started.store(true, std::memory_order_release);
    ordered_status = db_impl->WriteOrderedWithOutcome(
        WriteOptions(), &ordered_batch, nullptr, &ordered_state);
    ordered_done.store(true, std::memory_order_release);
  });
  while (!ordered_started.load(std::memory_order_acquire)) {
    options.env->SleepForMicroseconds(100);
  }
  while (!db_impl->TEST_OrderedWriteWaiting() &&
         !ordered_done.load(std::memory_order_acquire)) {
    options.env->SleepForMicroseconds(100);
  }
  EXPECT_TRUE(db_impl->TEST_OrderedWriteWaiting());
  EXPECT_FALSE(ordered_done.load(std::memory_order_acquire));

  env.ReleaseLogAppend();
  ordinary_writer.join();
  ordered_writer.join();

  EXPECT_TRUE(ordinary_status.ok()) << ordinary_status.ToString();
  EXPECT_TRUE(ordered_status.ok()) << ordered_status.ToString();
  EXPECT_EQ(DBImpl::WriteState::kApplied, ordered_state);
  EXPECT_EQ(2U, db_impl->LatestSequence());

  std::string value;
  ASSERT_TRUE(db->Get(ReadOptions(), "ordinary", &value).ok());
  EXPECT_EQ("first", value);
  ASSERT_TRUE(db->Get(ReadOptions(), "ordered", &value).ok());
  EXPECT_EQ("second", value);

  delete db;
  CleanDir(options.env, dbname);
}

TEST(DBIMPL_TEST, SyncDoesNotRotateUnsyncedWAL) {
  BlockingLogEnv env(Env::Default());
  Options options;
  options.env = &env;
  options.create_if_missing = true;
  options.write_buffer_size = 64 << 10;
  const std::string dbname("testdb-dbimpl-sync-no-rotate");
  CleanDir(options.env, dbname);

  DB* db = nullptr;
  ASSERT_TRUE(DB::Open(options, dbname, &db).ok());
  ASSERT_NE(nullptr, db);
  DBImpl* db_impl = static_cast<DBImpl*>(db);

  ASSERT_TRUE(
      db->Put(WriteOptions(), "large", std::string(128 << 10, 'x')).ok());
  const int logs_before_sync = env.LogFileCreations();
  ASSERT_EQ(1, logs_before_sync);

  Status s = db_impl->Sync();
  EXPECT_TRUE(s.ok()) << s.ToString();
  EXPECT_EQ(logs_before_sync, env.LogFileCreations());
  EXPECT_FALSE(env.ClosedDirtyLog());

  delete db;
  CleanDir(options.env, dbname);
}

TEST(DBIMPL_TEST, SyncWaitsForRetiredWALFlush) {
  BlockingLogEnv env(Env::Default());
  Options options;
  options.env = &env;
  options.create_if_missing = true;
  options.write_buffer_size = 64 << 10;
  const std::string dbname("testdb-dbimpl-sync-retired-wal");
  CleanDir(options.env, dbname);

  DB* db = nullptr;
  ASSERT_TRUE(DB::Open(options, dbname, &db).ok());
  ASSERT_NE(nullptr, db);
  DBImpl* db_impl = static_cast<DBImpl*>(db);
  ASSERT_TRUE(
      db->Put(WriteOptions(), "large", std::string(128 << 10, 'x')).ok());

  env.BlockTableSync();
  ASSERT_TRUE(db->Put(WriteOptions(), "rotate", "value").ok());
  env.WaitForBlockedTableSync();

  Status sync_status;
  std::atomic<bool> sync_done{false};
  std::thread syncer([&] {
    sync_status = db_impl->Sync();
    sync_done.store(true, std::memory_order_release);
  });
  while (db_impl->TEST_WriterQueueSize() == 0) {
    options.env->SleepForMicroseconds(100);
  }
  EXPECT_FALSE(sync_done.load(std::memory_order_acquire));

  env.ReleaseTableSync();
  syncer.join();
  EXPECT_TRUE(sync_status.ok()) << sync_status.ToString();

  delete db;
  CleanDir(options.env, dbname);
}

TEST(DBIMPL_TEST, SyncWriteWaitsForRetiredWALFlush) {
  BlockingLogEnv env(Env::Default());
  Options options;
  options.env = &env;
  options.create_if_missing = true;
  options.write_buffer_size = 64 << 10;
  const std::string dbname("testdb-dbimpl-sync-write-retired-wal");
  CleanDir(options.env, dbname);

  DB* db = nullptr;
  ASSERT_TRUE(DB::Open(options, dbname, &db).ok());
  ASSERT_NE(nullptr, db);
  ASSERT_TRUE(
      db->Put(WriteOptions(), "large", std::string(128 << 10, 'x')).ok());

  env.BlockTableSync();
  WriteOptions durable;
  durable.sync = true;
  Status write_status;
  std::atomic<bool> write_done{false};
  std::thread writer([&] {
    write_status = db->Put(durable, "sync", "value");
    write_done.store(true, std::memory_order_release);
  });
  env.WaitForBlockedTableSync();
  EXPECT_FALSE(write_done.load(std::memory_order_acquire));

  env.ReleaseTableSync();
  writer.join();
  EXPECT_TRUE(write_status.ok()) << write_status.ToString();

  delete db;
  CleanDir(options.env, dbname);
}

TEST(VLOG_TEST, AllStaleGCRequiresSuccessfulLSMSync) {
  BlockingLogEnv env(Env::Default());
  Options options;
  options.env = &env;
  options.create_if_missing = true;
  options.blob_value_size_threshold = 1;
  options.blob_max_file_size = 2048;
  options.blob_gc_interval = 0;
  const std::string dbname("testdb-gc-all-stale-sync");
  CleanDir(options.env, dbname);

  BlobDB* db = nullptr;
  ASSERT_TRUE(BlobDB::Open(options, dbname, &db).ok());
  WriteOptions durable;
  durable.sync = true;
  ASSERT_TRUE(db->Put(durable, "a", std::string(1100, 'a')).ok());
  ASSERT_TRUE(db->Put(durable, "b", std::string(1100, 'b')).ok());
  const std::string source = VLogFileName(dbname, 3);
  ASSERT_TRUE(options.env->FileExists(source));

  ASSERT_TRUE(db->Put(WriteOptions(), "a", "new-a").ok());
  ASSERT_TRUE(db->Put(WriteOptions(), "b", "new-b").ok());
  env.FailNextLogSync();
  db->ManualGC(3);
  db->WaitVLogGC();

  EXPECT_TRUE(db->VLogBGError().IsIOError())
      << db->VLogBGError().ToString();
  EXPECT_TRUE(options.env->FileExists(source));

  delete db;
  CleanDir(options.env, dbname);
}

TEST(VLOG_TEST, BecameStaleGCRequiresSuccessfulLSMSync) {
  REQUIRE_TEST_SYNC_POINTS();
  ScopedGCSyncPoints sync_points;
  BlockingLogEnv env(Env::Default());
  Options options;
  options.env = &env;
  options.create_if_missing = true;
  options.blob_value_size_threshold = 1;
  // Keep both candidates in vlog #3, then force it read-only with a third
  // record. The GC initially collects b, only for the sync-point overwrite
  // below to make that last copied entry stale before Rewrite().
  options.blob_max_file_size = 2500;
  options.blob_gc_interval = 0;
  const std::string dbname("testdb-gc-became-stale-sync");
  CleanDir(options.env, dbname);

  BlobDB* db = nullptr;
  ASSERT_TRUE(BlobDB::Open(options, dbname, &db).ok());
  WriteOptions durable;
  durable.sync = true;
  ASSERT_TRUE(db->Put(durable, "a", std::string(1100, 'a')).ok());
  ASSERT_TRUE(db->Put(durable, "b", std::string(1100, 'b')).ok());
  ASSERT_TRUE(
      db->Put(durable, "rollover", std::string(1100, 'r')).ok());
  const std::string source = VLogFileName(dbname, 3);
  ASSERT_TRUE(options.env->FileExists(source));
  ASSERT_TRUE(db->Put(WriteOptions(), "a", "new-a").ok());

  struct OverwriteArg {
    BlobDB* db;
    Status status;
  } overwrite{db, Status::OK()};
  auto overwrite_remaining = +[](void* raw) {
    auto* arg = reinterpret_cast<OverwriteArg*>(raw);
    arg->status = arg->db->Put(WriteOptions(), "b", "new-b");
    return false;
  };
  TEST_SYNC_POINT_ARG("GC.AfterCollect", &overwrite);
  TEST_SYNC_POINT_CALLBACK("GC.AfterCollect", overwrite_remaining);

  env.FailNextLogSync();
  db->ManualGC(3);
  db->WaitVLogGC();

  EXPECT_TRUE(overwrite.status.ok()) << overwrite.status.ToString();
  EXPECT_TRUE(db->VLogBGError().IsIOError())
      << db->VLogBGError().ToString();
  EXPECT_TRUE(options.env->FileExists(source));

  delete db;
  CleanDir(options.env, dbname);
}

TEST(VLOG_TEST, IndeterminateLSMAppendKeepsPublicationPin) {
  BlockingLogEnv env(Env::Default());
  Options options;
  options.env = &env;
  options.create_if_missing = true;
  options.blob_value_size_threshold = 1;
  options.blob_max_file_size = 512;
  options.blob_gc_interval = 0;
  const std::string dbname("testdb-indeterminate-publication");
  CleanDir(options.env, dbname);

  BlobDB* db = nullptr;
  ASSERT_TRUE(BlobDB::Open(options, dbname, &db).ok());
  env.FailLogAppendAfterSuccessOnCall(2);
  Status s = db->Put(WriteOptions(), "key", std::string(1100, 'v'));
  ASSERT_TRUE(s.IsIOError()) << s.ToString();
  Status later = db->Put(WriteOptions(), "later", std::string(1100, 'x'));
  EXPECT_TRUE(later.IsIOError()) << later.ToString();
  std::string unpublished;
  EXPECT_TRUE(db->Get(ReadOptions(), "later", &unpublished).IsNotFound());

  const std::string source = VLogFileName(dbname, 3);
  ASSERT_TRUE(options.env->FileExists(source));
  db->ManualGC(3);
  db->WaitVLogGC();
  EXPECT_TRUE(db->VLogBGError().ok()) << db->VLogBGError().ToString();
  db->RemoveObsoleteBlob();
  EXPECT_TRUE(options.env->FileExists(source));

  delete db;
  db = nullptr;
  ASSERT_TRUE(BlobDB::Open(options, dbname, &db).ok());
  std::string value;
  ASSERT_TRUE(db->Get(ReadOptions(), "key", &value).ok());
  EXPECT_EQ(std::string(1100, 'v'), value);

  delete db;
  CleanDir(options.env, dbname);
}

TEST(VLOG_TEST, CallbackIOErrorReleasesUnpublishedVLogPin) {
  BlockingLogEnv env(Env::Default());
  Options options;
  options.env = &env;
  options.create_if_missing = true;
  options.blob_value_size_threshold = 1;
  options.blob_max_file_size = 512;
  options.blob_gc_interval = 0;
  const std::string dbname("testdb-callback-error-publication");
  CleanDir(options.env, dbname);

  class RejectBeforeWAL : public WriteCallback {
   public:
    Status Callback(DB*) override {
      return Status::IOError("injected callback rejection");
    }
    bool AllowGrouping() const override { return false; }
  } callback;

  BlobDB* db = nullptr;
  ASSERT_TRUE(BlobDB::Open(options, dbname, &db).ok());
  WriteBatch batch;
  batch.Put("key", std::string(1100, 'v'));
  Status s = db->Write(WriteOptions(), &batch, &callback);
  ASSERT_TRUE(s.IsIOError()) << s.ToString();

  // Callback rejection is local and occurs before WAL emission. It must not
  // poison the ordered publication queue for an independent later request.
  ASSERT_TRUE(
      db->Put(WriteOptions(), "after-rejection", std::string(1100, 'a')).ok());
  std::string after_rejection;
  ASSERT_TRUE(
      db->Get(ReadOptions(), "after-rejection", &after_rejection).ok());
  EXPECT_EQ(std::string(1100, 'a'), after_rejection);

  const std::string unpublished_vlog = VLogFileName(dbname, 3);
  ASSERT_TRUE(options.env->FileExists(unpublished_vlog));
  db->ManualGC(3);
  db->WaitVLogGC();
  EXPECT_TRUE(db->VLogBGError().ok()) << db->VLogBGError().ToString();
  db->RemoveObsoleteBlob();
  EXPECT_FALSE(options.env->FileExists(unpublished_vlog));

  delete db;
  CleanDir(options.env, dbname);
}

TEST(VLOG_TEST, NonIOWALFailureKeepsPublicationPin) {
  BlockingLogEnv env(Env::Default());
  Options options;
  options.env = &env;
  options.create_if_missing = true;
  options.blob_value_size_threshold = 1;
  options.blob_max_file_size = 512;
  options.blob_gc_interval = 0;
  const std::string dbname("testdb-non-io-wal-publication");
  CleanDir(options.env, dbname);

  BlobDB* db = nullptr;
  ASSERT_TRUE(BlobDB::Open(options, dbname, &db).ok());
  env.FailLogAppendAfterSuccessWithInvalidArgumentOnCall(2);
  Status s = db->Put(WriteOptions(), "key", std::string(1100, 'v'));
  ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
  Status later = db->Put(WriteOptions(), "later", std::string(1100, 'x'));
  EXPECT_TRUE(later.IsInvalidArgument()) << later.ToString();

  const std::string source = VLogFileName(dbname, 3);
  ASSERT_TRUE(options.env->FileExists(source));
  db->ManualGC(3);
  db->WaitVLogGC();
  EXPECT_TRUE(db->VLogBGError().ok()) << db->VLogBGError().ToString();
  db->RemoveObsoleteBlob();
  EXPECT_TRUE(options.env->FileExists(source));

  delete db;
  db = nullptr;
  ASSERT_TRUE(BlobDB::Open(options, dbname, &db).ok());
  std::string value;
  ASSERT_TRUE(db->Get(ReadOptions(), "key", &value).ok());
  EXPECT_EQ(std::string(1100, 'v'), value);

  delete db;
  CleanDir(options.env, dbname);
}

TEST(VLOG_TEST, IndeterminateManifestSyncKeepsNewVLog) {
  BlockingLogEnv env(Env::Default());
  Options options;
  options.env = &env;
  options.create_if_missing = true;
  options.blob_value_size_threshold = 1;
  options.blob_max_file_size = 512;
  options.blob_gc_interval = 0;
  const std::string dbname("testdb-indeterminate-vlog-manifest");
  CleanDir(options.env, dbname);

  BlobDB* db = nullptr;
  ASSERT_TRUE(BlobDB::Open(options, dbname, &db).ok());
  env.FailNextVLogManifestSyncAfterSuccess();
  Status s = db->Put(WriteOptions(), "orphan", std::string(1100, 'v'));
  ASSERT_TRUE(s.IsIOError()) << s.ToString();

  // The failed rollover edit is indeterminate: it was durably appended to the
  // current manifest before the injected error was returned. Its new writable
  // vlog must remain pinned even though the in-memory edit was not applied.
  const std::string uncertain_vlog = VLogFileName(dbname, 4);
  ASSERT_TRUE(options.env->FileExists(uncertain_vlog));
  db->RemoveObsoleteBlob();
  EXPECT_TRUE(options.env->FileExists(uncertain_vlog));

  delete db;
  db = nullptr;
  ASSERT_TRUE(BlobDB::Open(options, dbname, &db).ok());
  ASSERT_TRUE(db->Put(WriteOptions(), "after", "recovered").ok());
  std::string value;
  ASSERT_TRUE(db->Get(ReadOptions(), "after", &value).ok());
  EXPECT_EQ("recovered", value);

  delete db;
  CleanDir(options.env, dbname);
}

TEST(DBIMPL_TEST, GroupedWritesPreserveCallerBatches) {
  BlockingLogEnv env(Env::Default());
  Options options;
  options.env = &env;
  options.create_if_missing = true;
  const std::string dbname("testdb-dbimpl-preserve-batches");
  CleanDir(options.env, dbname);

  DB* db = nullptr;
  ASSERT_TRUE(DB::Open(options, dbname, &db).ok());
  ASSERT_NE(nullptr, db);
  DBImpl* db_impl = static_cast<DBImpl*>(db);

  WriteBatch blocker_batch;
  blocker_batch.Put("blocker", "blocker-value");
  WriteBatch first_batch;
  first_batch.Put("first", "first-value");
  WriteBatch follower_batch;
  follower_batch.Put("follower", "follower-value");
  const std::string first_before =
      WriteBatchInternal::Contents(&first_batch).ToString();
  const std::string follower_before =
      WriteBatchInternal::Contents(&follower_batch).ToString();

  env.BlockNextLogAppend();
  Status blocker_status;
  Status first_status;
  Status follower_status;
  std::thread blocker([&] {
    blocker_status = db->Write(WriteOptions(), &blocker_batch);
  });
  env.WaitForBlockedLogAppend();

  std::thread first(
      [&] { first_status = db->Write(WriteOptions(), &first_batch); });
  std::thread follower(
      [&] { follower_status = db->Write(WriteOptions(), &follower_batch); });
  WaitForWriterQueueSize(db_impl, 3);
  env.ReleaseLogAppend();

  blocker.join();
  first.join();
  follower.join();

  EXPECT_TRUE(blocker_status.ok()) << blocker_status.ToString();
  EXPECT_TRUE(first_status.ok()) << first_status.ToString();
  EXPECT_TRUE(follower_status.ok()) << follower_status.ToString();
  EXPECT_EQ(first_before,
            WriteBatchInternal::Contents(&first_batch).ToString());
  EXPECT_EQ(follower_before,
            WriteBatchInternal::Contents(&follower_batch).ToString());

  std::string value;
  ASSERT_TRUE(db->Get(ReadOptions(), "first", &value).ok());
  EXPECT_EQ("first-value", value);
  ASSERT_TRUE(db->Get(ReadOptions(), "follower", &value).ok());
  EXPECT_EQ("follower-value", value);

  delete db;
  CleanDir(options.env, dbname);
}

TEST(DBIMPL_TEST, CallbackSegmentsDoNotDuplicateWrites) {
  BlockingLogEnv env(Env::Default());
  Options options;
  options.env = &env;
  options.create_if_missing = true;
  const std::string dbname("testdb-dbimpl-callback-segments");
  CleanDir(options.env, dbname);

  DB* db = nullptr;
  ASSERT_TRUE(DB::Open(options, dbname, &db).ok());
  ASSERT_NE(nullptr, db);
  DBImpl* db_impl = static_cast<DBImpl*>(db);

  class CountingCallback : public WriteCallback {
   public:
    Status Callback(DB* db) override {
      calls.fetch_add(1, std::memory_order_relaxed);
      std::string value;
      return db->Get(ReadOptions(), "first", &value);
    }
    bool AllowGrouping() const override { return true; }

    std::atomic<int> calls{0};
  } callback;

  WriteBatch blocker_batch;
  blocker_batch.Put("blocker", "0");
  WriteBatch first_batch;
  first_batch.Put("first", "1");
  WriteBatch follower_batch;
  follower_batch.Put("follower", "2");
  WriteBatch callback_batch;
  callback_batch.Put("callback", "3");
  WriteBatch after_callback_batch;
  after_callback_batch.Put("after", "4");

  env.BlockNextLogAppend();
  Status blocker_status;
  Status first_status;
  Status follower_status;
  Status callback_status;
  Status after_callback_status;
  std::thread blocker([&] {
    blocker_status = db->Write(WriteOptions(), &blocker_batch);
  });
  env.WaitForBlockedLogAppend();

  std::thread first(
      [&] { first_status = db->Write(WriteOptions(), &first_batch); });
  WaitForWriterQueueSize(db_impl, 2);
  std::thread follower(
      [&] { follower_status = db->Write(WriteOptions(), &follower_batch); });
  WaitForWriterQueueSize(db_impl, 3);
  std::thread callback_writer([&] {
    callback_status =
        db->Write(WriteOptions(), &callback_batch, &callback);
  });
  WaitForWriterQueueSize(db_impl, 4);
  std::thread after_callback([&] {
    after_callback_status =
        db->Write(WriteOptions(), &after_callback_batch);
  });
  WaitForWriterQueueSize(db_impl, 5);
  env.ReleaseLogAppend();

  blocker.join();
  first.join();
  follower.join();
  callback_writer.join();
  after_callback.join();

  EXPECT_TRUE(blocker_status.ok()) << blocker_status.ToString();
  EXPECT_TRUE(first_status.ok()) << first_status.ToString();
  EXPECT_TRUE(follower_status.ok()) << follower_status.ToString();
  EXPECT_TRUE(callback_status.ok()) << callback_status.ToString();
  EXPECT_TRUE(after_callback_status.ok())
      << after_callback_status.ToString();
  EXPECT_EQ(1, callback.calls.load(std::memory_order_relaxed));
  EXPECT_EQ(5U, db_impl->LatestSequence());

  for (const std::string& key :
       {"blocker", "first", "follower", "callback", "after"}) {
    std::string value;
    EXPECT_TRUE(db->Get(ReadOptions(), key, &value).ok()) << key;
  }

  delete db;
  CleanDir(options.env, dbname);
}

TEST(VLOG_TEST, DBWrapperIterator) {
  Options options;
  Status s;
  options.env->NewStdLogger(&options.info_log);
  options.create_if_missing = true;
  options.blob_max_file_size = 8 << 20;
  options.blob_value_size_threshold = 256;
  std::string dbname("testdb");
  std::string value;
  CleanDir(options.env, dbname);
  int num_ondisk_batches = 10000;
  int num_batches = num_ondisk_batches + 200;
  int per_batch = 100;

  BlobDB* db;
  BlobDB::Open(options, dbname, &db);

  std::unordered_map<std::string, std::string> kvmap;

  std::random_device rd;
  std::mt19937 mt(rd());
  std::uniform_int_distribution<int> dist(
      1, 2 * options.blob_value_size_threshold);
  for (int i = 0; i < num_ondisk_batches; ++i) {
    WriteBatch batch;
    for (int j = 0; j < per_batch; ++j) {
      std::string key = "key" + std::to_string(i * per_batch + j);
      key = std::to_string(std::hash<std::string>{}(key));
      std::string val = "value" + std::string(dist(mt), 'x');
      kvmap[key] = val;
      batch.Put(key, val);
    }
    s = db->Write(WriteOptions(), &batch);
    ASSERT_TRUE(s.ok());
  }

  for (int i = num_ondisk_batches; i < num_batches; ++i) {
    WriteBatch batch;
    for (int j = 0; j < per_batch; ++j) {
      std::string key = "key" + std::to_string(i * per_batch + j);
      key = std::to_string(std::hash<std::string>{}(key));
      std::string val = "value" + std::string(dist(mt), 'x');
      kvmap[key] = val;
      batch.Put(key, val);
    }
    s = db->Write(WriteOptions(), &batch);
    ASSERT_TRUE(s.ok());
  }

  Iterator* iter = db->NewIterator(ReadOptions());

  int reverse_point = num_batches * per_batch / 2;
  for (iter->SeekToFirst(); iter->Valid() && reverse_point > 0;
       iter->Next(), reverse_point--) {
    std::string expected = kvmap[iter->key().ToString()];
    ASSERT_EQ(iter->value(), expected);
  }
  for (; iter->Valid(); iter->Prev()) {
    std::string expected = kvmap[iter->key().ToString()];
    ASSERT_EQ(iter->value(), expected);
  }

  reverse_point = num_batches * per_batch / 2;
  for (iter->SeekToLast(); iter->Valid(), reverse_point > 0;
       iter->Prev(), reverse_point--) {
    std::string expected = kvmap[iter->key().ToString()];
    ASSERT_EQ(iter->value(), expected);
  }
  for (; iter->Valid(); iter->Next()) {
    std::string expected = kvmap[iter->key().ToString()];
    ASSERT_EQ(iter->value(), expected);
  }
  delete iter;

  printf("%s", reinterpret_cast<BlobDB*>(db)->DebugString().c_str());
  delete db;
  CleanDir(options.env, dbname);
}

TEST(VLOG_TEST, DBWrapperWriteBatch) {
  Options options;
  Status s;
  options.env->NewStdLogger(&options.info_log);
  options.create_if_missing = true;
  options.blob_max_file_size = 8 << 20;
  options.blob_value_size_threshold = 256;
  std::string dbname("testdb");
  std::string value;
  CleanDir(options.env, dbname);
  int num_ondisk_batches = 1000;
  int num_batches = num_ondisk_batches + 200;
  int per_batch = 100;

  BlobDB* db;
  BlobDB::Open(options, dbname, &db);

  std::unordered_map<std::string, std::string> kvmap;

  std::random_device rd;
  std::mt19937 mt(rd());
  std::uniform_int_distribution<int> dist(
      1, 2 * options.blob_value_size_threshold);
  for (int i = 0; i < num_ondisk_batches; ++i) {
    WriteBatch batch;
    for (int j = 0; j < per_batch; ++j) {
      std::string key = "key" + std::to_string(i * per_batch + j);
      std::string val = "value" + std::string(dist(mt), 'x');
      kvmap[key] = val;
      batch.Put(key, val);
    }
    s = db->Write(WriteOptions(), &batch);
    ASSERT_TRUE(s.ok());
  }

  for (int i = num_ondisk_batches; i < num_batches; ++i) {
    WriteBatch batch;
    for (int j = 0; j < per_batch; ++j) {
      std::string key = "key" + std::to_string(i * per_batch + j);
      std::string val = "value" + std::string(dist(mt), 'x');
      kvmap[key] = val;
      batch.Put(key, val);
    }
    s = db->Write(WriteOptions(), &batch);
    ASSERT_TRUE(s.ok());
  }

  const std::string begin_storage = "key" + std::to_string(0);
  const std::string end_storage =
      "key" + std::to_string(num_ondisk_batches * per_batch - 1);
  Slice begin(begin_storage);
  Slice end(end_storage);
  db->CompactRange(&begin, &end);

  for (auto& p : kvmap) {
    s = db->Get(ReadOptions(), p.first, &value);
    ASSERT_TRUE(s.ok());
    ASSERT_EQ(value, p.second);
  }

  printf("%s", reinterpret_cast<BlobDB*>(db)->DebugString().c_str());
  delete db;
  CleanDir(options.env, dbname);
}

TEST(VLOG_TEST, WriteBatchPreservesRepeatedKeyOrder) {
  Options options;
  options.create_if_missing = true;
  options.blob_value_size_threshold = 16;
  options.blob_gc_interval = 0;
  options.blob_background_read_threads = 0;
  const std::string dbname("testdb-write-batch-order");
  CleanDir(options.env, dbname);

  BlobDB* raw_db = nullptr;
  ASSERT_TRUE(BlobDB::Open(options, dbname, &raw_db).ok());
  std::unique_ptr<BlobDB> db(raw_db);
  const std::string large_value(1024, 'x');

  WriteBatch put_then_delete;
  put_then_delete.Put("deleted", large_value);
  put_then_delete.Delete("deleted");
  ASSERT_TRUE(db->Write(WriteOptions(), &put_then_delete).ok());

  std::string value;
  EXPECT_TRUE(db->Get(ReadOptions(), "deleted", &value).IsNotFound());

  WriteBatch delete_then_put;
  delete_then_put.Delete("present");
  delete_then_put.Put("present", large_value);
  ASSERT_TRUE(db->Write(WriteOptions(), &delete_then_put).ok());
  ASSERT_TRUE(db->Get(ReadOptions(), "present", &value).ok());
  EXPECT_EQ(large_value, value);

  db.reset();
  CleanDir(options.env, dbname);
}

TEST(VLOG_TEST, PointGetRejectsTrailingValueHandleBytes) {
  Options options;
  options.create_if_missing = true;
  options.blob_gc_interval = 0;
  options.blob_background_read_threads = 0;
  const std::string dbname("testdb-trailing-value-handle");
  CleanDir(options.env, dbname);

  DB* raw_lsm = nullptr;
  ASSERT_TRUE(DB::Open(options, dbname, &raw_lsm).ok());
  std::unique_ptr<DB> lsm(raw_lsm);

  ValueHandle handle(3, 0, 0, 12);
  std::string malformed_handle;
  handle.EncodeTo(&malformed_handle);
  malformed_handle.append("trailing-bytes");
  WriteBatch batch;
  WriteBatchInternal::Put(&batch, "key", malformed_handle,
                          kTypeValueHandle);
  ASSERT_TRUE(lsm->Write(WriteOptions(), &batch).ok());
  lsm.reset();

  BlobDB* raw_blob_db = nullptr;
  ASSERT_TRUE(BlobDB::Open(options, dbname, &raw_blob_db).ok());
  std::unique_ptr<BlobDB> blob_db(raw_blob_db);
  std::string value;
  const Status s = blob_db->Get(ReadOptions(), "key", &value);
  EXPECT_TRUE(s.IsCorruption()) << s.ToString();

  blob_db.reset();
  CleanDir(options.env, dbname);
}

TEST(VLOG_TEST, DBWrapperNoGC) {
  Options options;
  Status s;
  options.env->NewStdLogger(&options.info_log);
  options.create_if_missing = true;
  options.blob_max_file_size = 8 << 20;
  options.blob_value_size_threshold = 256;
  std::string dbname("testdb");
  std::string value;
  CleanDir(options.env, dbname);
  int num_ondisk_entries = 100000;
  int num_entries = num_ondisk_entries + 20000;

  BlobDB* db;
  BlobDB::Open(options, dbname, &db);

  s = db->Put(WriteOptions(), "key1", "value1");
  ASSERT_TRUE(s.ok());
  s = db->Get(ReadOptions(), "key1", &value);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(value, "value1");

  s = db->Put(WriteOptions(), "key2", std::string(100, 'x'));
  ASSERT_TRUE(s.ok());
  s = db->Get(ReadOptions(), "key2", &value);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(value, std::string(100, 'x'));

  std::unordered_map<std::string, std::string> kvmap;

  std::random_device rd;
  std::mt19937 mt(rd());
  std::uniform_int_distribution<int> dist(
      1, 2 * options.blob_value_size_threshold);
  for (int i = 0; i < num_ondisk_entries; ++i) {
    std::string key = "key" + std::to_string(i);
    std::string val = "value" + std::string(dist(mt), 'x');
    s = db->Put(WriteOptions(), key, val);
    ASSERT_TRUE(s.ok());
    kvmap[key] = val;
  }

  for (int i = num_ondisk_entries; i < num_entries; ++i) {
    std::string key = "key" + std::to_string(i);
    std::string val = "value" + std::string(dist(mt), 'x');
    s = db->Put(WriteOptions(), key, val);
    ASSERT_TRUE(s.ok());
    kvmap[key] = val;
  }

  const std::string begin_storage = "key" + std::to_string(0);
  const std::string end_storage =
      "key" + std::to_string(num_ondisk_entries - 1);
  Slice begin(begin_storage);
  Slice end(end_storage);
  db->CompactRange(&begin, &end);

  for (auto& p : kvmap) {
    s = db->Get(ReadOptions(), p.first, &value);
    ASSERT_TRUE(s.ok());
    ASSERT_EQ(value, p.second);
  }

  s = db->Delete(WriteOptions(), "key1");
  ASSERT_TRUE(s.ok());
  s = db->Get(ReadOptions(), "key1", &value);
  ASSERT_TRUE(s.IsNotFound());

  printf("%s", reinterpret_cast<BlobDB*>(db)->DebugString().c_str());
  delete db;
  CleanDir(options.env, dbname);
}

TEST(VLOG_TEST, ValueLogRecover) {
  Options options;
  Status s;
  options.env->NewStdLogger(&options.info_log);
  options.create_if_missing = true;
  options.blob_max_file_size = 8 << 20;
  std::string dbname("testdb");
  CleanDir(options.env, dbname);

  DB* db;
  DB::Open(options, dbname, &db);
  // we do not need to start a real DBWrapper instance in this test
  BlobDB* dbwrapper = reinterpret_cast<BlobDB*>(0x1234);
  ValueLogImpl* v;
  ValueLogImpl::Open(options, dbname, dbwrapper, &v);

  ValueHandle handle;
  v->Put(WriteOptions(), "k01", "value01", &handle);
  ASSERT_EQ(handle, ValueHandle(3, 0, 0, 12));
  v->Put(WriteOptions(), "k02", "value02", &handle);
  ASSERT_EQ(handle, ValueHandle(3, 0, 12, 12));
  v->Put(WriteOptions(), "k03", "value03", &handle);
  ASSERT_EQ(handle, ValueHandle(3, 0, 24, 12));

  delete v;

  ValueLogImpl::Open(options, dbname, dbwrapper, &v);

  std::string value;
  v->Get(ReadOptions(), ValueHandle(3, 0, 0, 12), &value);
  ASSERT_EQ(value, "value01");
  v->Get(ReadOptions(), ValueHandle(3, 0, 12, 12), &value);
  ASSERT_EQ(value, "value02");
  v->Get(ReadOptions(), ValueHandle(3, 0, 24, 0), &value);
  ASSERT_EQ(value, "value03");

  v->Put(WriteOptions(), "k04", "value04", &handle);
  ASSERT_EQ(handle, ValueHandle(3, 0, 36, 12));
  v->Put(WriteOptions(), "k05", "value05", &handle);
  ASSERT_EQ(handle, ValueHandle(3, 0, 48, 12));
  v->Put(WriteOptions(), "k06", "value06", &handle);
  ASSERT_EQ(handle, ValueHandle(3, 0, 60, 12));

  // simulate broken .vlog file with last few records lost caused by OS crash
  for (int i = 60; i < 72; i++) {
    delete v;
    options.env->TruncateFile(VLogFileName(dbname, 3), i);
    ValueLogImpl::Open(options, dbname, dbwrapper, &v);

    v->Put(WriteOptions(), "k06", "value06", &handle);
    ASSERT_EQ(handle, ValueHandle(3, 0, 60, 12));
  }

  uint32_t size = 72;
  uint32_t num_entries = 6;
  for (int i = 0; size <= options.blob_max_file_size / 2; i++) {
    const std::string key_storage = "k0" + std::to_string(i + 7);
    const std::string val_storage = "value0" + std::to_string(i + 7);
    Slice key(key_storage);
    Slice val(val_storage);
    v->Put(WriteOptions(), key, val, &handle);
    ASSERT_EQ(handle, ValueHandle(3, 0, size, SizeOf(key, val)));
    size += SizeOf(key, val);
    num_entries++;
  }

  delete v;
  ValueLogImpl::Open(options, dbname, dbwrapper, &v);

  size = 0;
  for (int i = 1; i <= num_entries; i++) {
    const std::string key_storage = "k1" + std::to_string(i);
    const std::string val_storage = "value1" + std::to_string(i);
    Slice key(key_storage);
    Slice val(val_storage);
    v->Put(WriteOptions(), key, val, &handle);
    ASSERT_EQ(handle, ValueHandle(18, 0, size, SizeOf(key, val)));
    size += SizeOf(key, val);
  }

  size = 0;
  for (int i = 1; i <= num_entries; i++) {
    const std::string key_storage = "k1" + std::to_string(i);
    const std::string val_storage = "value1" + std::to_string(i);
    Slice key(key_storage);
    Slice val(val_storage);
    s = v->Get(ReadOptions(), ValueHandle(18, 0, size, SizeOf(key, val)),
               &value);
    ASSERT_TRUE(s.ok());
    ASSERT_EQ(Slice(value), val);
    size += SizeOf(key, val);
  }

  size = 0;
  for (int i = 1; i <= 1000000; i++) {
    const std::string key_storage = "k1" + std::to_string(i);
    const std::string val_storage = "value1" + std::to_string(i);
    Slice key(key_storage);
    Slice val(val_storage);
    s = v->Put(WriteOptions(), key, val, &handle);
    ASSERT_TRUE(s.ok());
    size += SizeOf(key, val);
  }

  printf("%s", v->DebugString().c_str());

  delete v;
  delete db;
  CleanDir(options.env, dbname);
}

TEST(VLOG_TEST, ConcurrentSPMC) {
  Options options;
  Status s;
  options.env->NewStdLogger(&options.info_log);
  options.create_if_missing = true;
  options.blob_max_file_size = 8 << 20;
  std::string dbname("testdb");
  CleanDir(options.env, dbname);

  BlobDB* db = nullptr;
  ASSERT_TRUE(BlobDB::Open(options, dbname, &db).ok());
  std::deque<std::pair<std::string, std::string>> kvq;
  port::Mutex lk;
  port::CondVar cv(&lk);
  uint32_t total_entries = 8 * 20000;
  int n_writers = 8;  // single-producer
  int n_readers = 8;  // multi-consumer

  total_entries = n_writers * (total_entries / n_writers);
  total_entries = n_readers * (total_entries / n_readers);
  uint32_t per_writer = total_entries / n_writers;
  uint32_t per_reader = total_entries / n_readers;

  std::thread** wth = new std::thread*[n_writers];
  std::thread** rth = new std::thread*[n_readers];

  for (int i = 0; i < n_writers; i++) {
    wth[i] = new std::thread(
        [&lk, &kvq, db, &cv, per_writer](int k) {
          for (int j = k * per_writer; j < (k + 1) * per_writer; ++j) {
            std::string key("k0" + std::to_string(j));
            std::string val("value0" + std::to_string(j) +
                            std::string(1024, 'x'));
            auto s = db->Put(WriteOptions(), key, val);
            ASSERT_TRUE(s.ok()) << s.ToString();
            lk.Lock();
            kvq.emplace_back(key, val);
            lk.Unlock();
            cv.SignalAll();
          }
          return;
        },
        i);
  }

  for (int i = 0; i < n_readers; i++) {
    rth[i] = new std::thread(
        [&lk, &kvq, db, &cv, per_reader](int k) {
          std::string key;
          std::string val;
          std::string expected;
          for (int j = k * per_reader; j < (k + 1) * per_reader; ++j) {
            lk.Lock();
            while (kvq.empty()) {
              cv.Wait();
            }
            key = kvq.front().first;
            expected = kvq.front().second;
            kvq.pop_front();
            lk.Unlock();
            auto s = db->Get(ReadOptions(), key, &val);
            ASSERT_TRUE(s.ok()) << s.ToString();
            ASSERT_EQ(val, expected);
          }
          return;
        },
        i);
  }

  for (int i = 0; i < n_writers; i++) {
    wth[i]->join();
    delete wth[i];
  }
  for (int i = 0; i < n_readers; i++) {
    rth[i]->join();
    delete rth[i];
  }

  printf("%s", db->DebugString().c_str());

  delete db;
  delete[] wth;
  delete[] rth;
  CleanDir(options.env, dbname);
}

namespace {

class QueuedScheduleEnv : public EnvWrapper {
 public:
  explicit QueuedScheduleEnv(Env* target) : EnvWrapper(target) {}

  void Schedule(void (*function)(void*), void* arg) override {
    {
      std::lock_guard<std::mutex> lock(mu_);
      jobs_.push_back(Job(function, arg));
    }
    cv_.notify_all();
  }

  bool WaitForJob(std::chrono::milliseconds timeout) {
    std::unique_lock<std::mutex> lock(mu_);
    return cv_.wait_for(lock, timeout, [this] { return !jobs_.empty(); });
  }

  bool RunNext() {
    Job job;
    {
      std::lock_guard<std::mutex> lock(mu_);
      if (jobs_.empty()) return false;
      job = jobs_.front();
      jobs_.pop_front();
    }
    job.function(job.arg);
    return true;
  }

 private:
  struct Job {
    Job() : function(nullptr), arg(nullptr) {}
    Job(void (*scheduled_function)(void*), void* scheduled_arg)
        : function(scheduled_function), arg(scheduled_arg) {}

    void (*function)(void*);
    void* arg;
  };

  std::mutex mu_;
  std::condition_variable cv_;
  std::deque<Job> jobs_;
};

class ObservedMemoryVLogFile : public AppendableRandomAccessFile {
 public:
  Status Read(uint64_t offset, size_t n, Slice* result,
              char* scratch) const override {
    std::lock_guard<std::mutex> lock(mu_);
    if (offset > contents_.size() || n > contents_.size() - offset) {
      *result = Slice();
      return Status::InvalidArgument("memory vlog read past end");
    }
    std::memcpy(scratch, contents_.data() + offset, n);
    *result = Slice(scratch, n);
    return Status::OK();
  }

  Status Append(const Slice& data) override {
    const Slice part(data);
    return WriteAtV(Offset(), &part, 1);
  }

  Status AppendV(const Slice* parts, size_t count) override {
    return WriteAtV(Offset(), parts, count);
  }

  Status WriteAtV(uint64_t offset, const Slice* parts,
                  size_t count) override {
    std::lock_guard<std::mutex> lock(mu_);
    if (closed_) return Status::IOError("memory vlog is closed");
    if (parts == nullptr && count != 0) {
      return Status::InvalidArgument("null memory vlog write vector");
    }
    uint64_t end = offset;
    for (size_t i = 0; i < count; ++i) {
      if (parts[i].size() > std::numeric_limits<uint64_t>::max() - end) {
        return Status::InvalidArgument("memory vlog is too large");
      }
      end += parts[i].size();
    }
    if (end > std::numeric_limits<size_t>::max()) {
      return Status::InvalidArgument("memory vlog is too large");
    }
    if (contents_.size() < end) contents_.resize(static_cast<size_t>(end));
    size_t destination = static_cast<size_t>(offset);
    for (size_t i = 0; i < count; ++i) {
      if (!parts[i].empty()) {
        std::memcpy(&contents_[destination], parts[i].data(), parts[i].size());
      }
      destination += parts[i].size();
    }
    return Status::OK();
  }

  Status Truncate(uint64_t size) override {
    std::lock_guard<std::mutex> lock(mu_);
    if (size > std::numeric_limits<size_t>::max()) {
      return Status::InvalidArgument("memory vlog is too large");
    }
    contents_.resize(static_cast<size_t>(size));
    return Status::OK();
  }

  Status Close() override {
    std::lock_guard<std::mutex> lock(mu_);
    closed_ = true;
    return Status::OK();
  }

  Status Flush() override { return Status::OK(); }

  Status Sync() override {
    bool fail;
    {
      std::lock_guard<std::mutex> lock(mu_);
      ++sync_calls_;
      fail = fail_sync_;
    }
    cv_.notify_all();
    return fail ? Status::IOError("injected background vlog sync error")
                : Status::OK();
  }

  uint64_t Offset() override {
    std::lock_guard<std::mutex> lock(mu_);
    return contents_.size();
  }

  void FailSync() {
    std::lock_guard<std::mutex> lock(mu_);
    fail_sync_ = true;
  }

  bool WaitForSyncCalls(int expected, std::chrono::milliseconds timeout) {
    std::unique_lock<std::mutex> lock(mu_);
    return cv_.wait_for(lock, timeout,
                        [this, expected] { return sync_calls_ >= expected; });
  }

 private:
  mutable std::mutex mu_;
  std::condition_variable cv_;
  std::string contents_;
  int sync_calls_{0};
  bool fail_sync_{false};
  bool closed_{false};
};

class BlockingAppendVLogEnv : public EnvWrapper {
 public:
  explicit BlockingAppendVLogEnv(Env* target) : EnvWrapper(target) {}

  Status NewAppendableRandomAccessFile(
      const std::string& filename,
      AppendableRandomAccessFile** result) override {
    AppendableRandomAccessFile* target_file = nullptr;
    Status s = target()->NewAppendableRandomAccessFile(filename, &target_file);
    if (!s.ok()) {
      *result = nullptr;
      return s;
    }
    *result = new File(this, target_file);
    return Status::OK();
  }

  void BlockNextAppendV() {
    std::lock_guard<std::mutex> lock(mu_);
    block_next_ = true;
    blocked_ = false;
    release_ = false;
  }

  bool WaitForBlockedAppendV(std::chrono::milliseconds timeout) {
    std::unique_lock<std::mutex> lock(mu_);
    return cv_.wait_for(lock, timeout, [this] { return blocked_; });
  }

  void ReleaseAppendV() {
    {
      std::lock_guard<std::mutex> lock(mu_);
      release_ = true;
    }
    cv_.notify_all();
  }

 private:
  class File : public AppendableRandomAccessFile {
   public:
    File(BlockingAppendVLogEnv* env, AppendableRandomAccessFile* target)
        : env_(env), target_(target) {}
    ~File() override { delete target_; }

    Status Read(uint64_t offset, size_t n, Slice* result,
                char* scratch) const override {
      return target_->Read(offset, n, result, scratch);
    }
    Status Append(const Slice& data) override { return target_->Append(data); }
    Status AppendV(const Slice* parts, size_t count) override {
      {
        std::unique_lock<std::mutex> lock(env_->mu_);
        if (env_->block_next_) {
          env_->block_next_ = false;
          env_->blocked_ = true;
          env_->cv_.notify_all();
          env_->cv_.wait(lock, [this] { return env_->release_; });
          env_->blocked_ = false;
        }
      }
      return target_->AppendV(parts, count);
    }
    Status WriteAtV(uint64_t offset, const Slice* parts,
                    size_t count) override {
      {
        std::unique_lock<std::mutex> lock(env_->mu_);
        if (env_->block_next_) {
          env_->block_next_ = false;
          env_->blocked_ = true;
          env_->cv_.notify_all();
          env_->cv_.wait(lock, [this] { return env_->release_; });
          env_->blocked_ = false;
        }
      }
      return target_->WriteAtV(offset, parts, count);
    }
    Status Truncate(uint64_t size) override {
      return target_->Truncate(size);
    }
    Status Close() override { return target_->Close(); }
    Status Flush() override { return target_->Flush(); }
    Status Sync() override { return target_->Sync(); }
    uint64_t Offset() override { return target_->Offset(); }

   private:
    BlockingAppendVLogEnv* const env_;
    AppendableRandomAccessFile* const target_;
  };

  std::mutex mu_;
  std::condition_variable cv_;
  bool block_next_{false};
  bool blocked_{false};
  bool release_{false};
};

class ErrorInjectingVLogFile : public AppendableRandomAccessFile {
 public:
  ErrorInjectingVLogFile(AppendableRandomAccessFile* target,
                         std::atomic<bool>* fail_append,
                         std::atomic<bool>* fail_sync)
      : target_(target),
        fail_append_(fail_append),
        fail_sync_(fail_sync) {}

  ~ErrorInjectingVLogFile() override { delete target_; }

  Status Read(uint64_t offset, size_t n, Slice* result,
              char* scratch) const override {
    return target_->Read(offset, n, result, scratch);
  }

  Status Append(const Slice& data) override {
    if (fail_append_->load(std::memory_order_acquire)) {
      return Status::IOError("injected vlog append error");
    }
    return target_->Append(data);
  }

  Status AppendV(const Slice* parts, size_t count) override {
    if (fail_append_->load(std::memory_order_acquire)) {
      return Status::IOError("injected vlog append error");
    }
    return target_->AppendV(parts, count);
  }

  Status WriteAtV(uint64_t offset, const Slice* parts,
                  size_t count) override {
    if (fail_append_->load(std::memory_order_acquire)) {
      return Status::IOError("injected vlog append error");
    }
    return target_->WriteAtV(offset, parts, count);
  }

  Status Truncate(uint64_t size) override {
    return target_->Truncate(size);
  }

  Status Close() override { return target_->Close(); }
  Status Flush() override { return target_->Flush(); }

  Status Sync() override {
    if (fail_sync_->load(std::memory_order_acquire)) {
      return Status::IOError("injected vlog sync error");
    }
    return target_->Sync();
  }

  uint64_t Offset() override { return target_->Offset(); }

 private:
  AppendableRandomAccessFile* const target_;
  std::atomic<bool>* const fail_append_;
  std::atomic<bool>* const fail_sync_;
};

class ErrorInjectingVLogEnv : public EnvWrapper {
 public:
  explicit ErrorInjectingVLogEnv(Env* target) : EnvWrapper(target) {}

  Status NewAppendableRandomAccessFile(
      const std::string& filename,
      AppendableRandomAccessFile** result) override {
    AppendableRandomAccessFile* target_file = nullptr;
    Status s = target()->NewAppendableRandomAccessFile(filename, &target_file);
    if (!s.ok()) {
      *result = nullptr;
      return s;
    }
    *result = new ErrorInjectingVLogFile(target_file, &fail_append, &fail_sync);
    return Status::OK();
  }

  std::atomic<bool> fail_append{false};
  std::atomic<bool> fail_sync{false};
};

// Blocks the first positioned write, partially writes and fails the second,
// and lets any later positioned writes complete. This makes out-of-order
// completion and the failed-range boundary deterministic.
class OrderedFailureVLogEnv : public EnvWrapper {
 public:
  explicit OrderedFailureVLogEnv(Env* target) : EnvWrapper(target) {}

  Status NewAppendableRandomAccessFile(
      const std::string& filename,
      AppendableRandomAccessFile** result) override {
    AppendableRandomAccessFile* target_file = nullptr;
    Status s = target()->NewAppendableRandomAccessFile(filename, &target_file);
    if (!s.ok()) {
      *result = nullptr;
      return s;
    }
    *result = new File(this, target_file);
    return Status::OK();
  }

  bool WaitForFirstWrite(std::chrono::milliseconds timeout) {
    std::unique_lock<std::mutex> lock(mu_);
    return cv_.wait_for(lock, timeout, [this] { return first_blocked_; });
  }

  bool WaitForFailedWrite(std::chrono::milliseconds timeout) {
    std::unique_lock<std::mutex> lock(mu_);
    return cv_.wait_for(lock, timeout, [this] { return failed_write_done_; });
  }

  bool WaitForSecondWrite(std::chrono::milliseconds timeout) {
    std::unique_lock<std::mutex> lock(mu_);
    return cv_.wait_for(lock, timeout, [this] { return second_waiting_; });
  }

  void ReleaseFirstWrite() {
    {
      std::lock_guard<std::mutex> lock(mu_);
      release_first_ = true;
      // Also make failure-path cleanup unable to strand the deliberately
      // delayed second call if the test never manages to start call three.
      third_write_started_ = true;
    }
    cv_.notify_all();
  }

  int max_active_writes() {
    std::lock_guard<std::mutex> lock(mu_);
    return max_active_writes_;
  }

 private:
  class File : public AppendableRandomAccessFile {
   public:
    File(OrderedFailureVLogEnv* env, AppendableRandomAccessFile* target)
        : env_(env), target_(target) {}
    ~File() override { delete target_; }

    Status Read(uint64_t offset, size_t n, Slice* result,
                char* scratch) const override {
      return target_->Read(offset, n, result, scratch);
    }
    Status Append(const Slice& data) override { return target_->Append(data); }
    Status AppendV(const Slice* parts, size_t count) override {
      return target_->AppendV(parts, count);
    }

    Status WriteAtV(uint64_t offset, const Slice* parts,
                    size_t count) override {
      bool fail = false;
      {
        std::unique_lock<std::mutex> lock(env_->mu_);
        const size_t write_index = env_->next_write_++;
        ++env_->active_writes_;
        env_->max_active_writes_ =
            std::max(env_->max_active_writes_, env_->active_writes_);
        if (write_index == 0) {
          env_->first_blocked_ = true;
          env_->cv_.notify_all();
          env_->cv_.wait(lock, [this] { return env_->release_first_; });
        } else if (write_index == 1) {
          fail = true;
          env_->second_waiting_ = true;
          env_->cv_.notify_all();
          env_->cv_.wait(lock, [this] { return env_->third_write_started_; });
        } else if (write_index == 2) {
          env_->third_write_started_ = true;
          env_->cv_.notify_all();
        }
      }

      Status s;
      if (fail) {
        // Model pwritev's documented partial-write failure. Leave one byte of
        // the failed record on disk so orderly close/recovery must repair it.
        size_t part = 0;
        while (part < count && parts[part].empty()) ++part;
        if (part < count) {
          Slice prefix(parts[part].data(), 1);
          s = target_->WriteAtV(offset, &prefix, 1);
        }
        if (s.ok()) s = Status::IOError("injected middle vlog write failure");
      } else {
        s = target_->WriteAtV(offset, parts, count);
      }

      {
        std::lock_guard<std::mutex> lock(env_->mu_);
        if (fail) env_->failed_write_done_ = true;
        --env_->active_writes_;
      }
      env_->cv_.notify_all();
      return s;
    }

    Status Truncate(uint64_t size) override {
      return target_->Truncate(size);
    }
    Status Close() override { return target_->Close(); }
    Status Flush() override { return target_->Flush(); }
    Status Sync() override { return target_->Sync(); }
    uint64_t Offset() override { return target_->Offset(); }

   private:
    OrderedFailureVLogEnv* const env_;
    AppendableRandomAccessFile* const target_;
  };

  std::mutex mu_;
  std::condition_variable cv_;
  size_t next_write_{0};
  bool first_blocked_{false};
  bool release_first_{false};
  bool second_waiting_{false};
  bool third_write_started_{false};
  bool failed_write_done_{false};
  int active_writes_{0};
  int max_active_writes_{0};
};

}  // namespace

TEST(VLOG_TEST, FinishWaitsForQueuedBackgroundSync) {
  QueuedScheduleEnv env(Env::Default());
  Options options;
  options.env = &env;
  options.blob_max_file_size = 64ULL << 20;

  auto* memory_file = new ObservedMemoryVLogFile;
  VLogRWFile* file = new VLogRWFile(options, memory_file, 3);
  file->Ref();
  ASSERT_TRUE(file->status().ok()) << file->status().ToString();

  ValueBatch batch;
  batch.Put("key", std::string(16 << 20, 'v'));
  uint32_t offset = 0;
  ASSERT_TRUE(file->Reserve(batch.size(), batch.NumEntries(), &offset).ok());
  ASSERT_TRUE(file->Write(offset, &batch).ok());
  ASSERT_TRUE(env.WaitForJob(std::chrono::seconds(5)));

  Status finish_status;
  std::atomic<bool> finish_done{false};
  std::thread finisher([&] {
    finish_status = file->Finish();
    finish_done.store(true, std::memory_order_release);
  });
  const bool finish_sync_started =
      memory_file->WaitForSyncCalls(1, std::chrono::seconds(5));
  const bool waited_for_background =
      !finish_done.load(std::memory_order_acquire);
  const bool ran_background = env.RunNext();
  finisher.join();

  EXPECT_TRUE(finish_sync_started);
  EXPECT_TRUE(waited_for_background);
  EXPECT_TRUE(ran_background);
  EXPECT_TRUE(finish_status.ok()) << finish_status.ToString();
  EXPECT_TRUE(finish_done.load(std::memory_order_acquire));

  file->Unref();
}

TEST(VLOG_TEST, BackgroundSyncFailureIsTerminal) {
  QueuedScheduleEnv env(Env::Default());
  Options options;
  options.env = &env;
  options.blob_max_file_size = 64ULL << 20;

  auto* memory_file = new ObservedMemoryVLogFile;
  VLogRWFile* file = new VLogRWFile(options, memory_file, 3);
  file->Ref();
  ASSERT_TRUE(file->status().ok()) << file->status().ToString();

  ValueBatch batch;
  batch.Put("key", std::string(16 << 20, 'v'));
  uint32_t offset = 0;
  ASSERT_TRUE(file->Reserve(batch.size(), batch.NumEntries(), &offset).ok());
  ASSERT_TRUE(file->Write(offset, &batch).ok());
  ASSERT_TRUE(env.WaitForJob(std::chrono::seconds(5)));

  memory_file->FailSync();
  ASSERT_TRUE(env.RunNext());
  EXPECT_TRUE(file->status().IsIOError()) << file->status().ToString();
  Status finish_status = file->Finish();
  EXPECT_TRUE(finish_status.IsIOError()) << finish_status.ToString();

  file->Unref();
}

TEST(VLOG_TEST, ParallelWritesPublishOnlyContiguousSuccessfulPrefix) {
  OrderedFailureVLogEnv env(Env::Default());
  Options options;
  options.env = &env;
  options.create_if_missing = true;
  options.blob_value_size_threshold = 1;
  options.blob_max_file_size = 1 << 20;
  options.blob_gc_interval = 0;
  options.blob_background_read_threads = 0;
  const std::string dbname("testdb-parallel-vlog-failure-boundary");
  CleanDir(options.env, dbname);

  BlobDB* db = nullptr;
  ASSERT_TRUE(BlobDB::Open(options, dbname, &db).ok());
  ASSERT_NE(nullptr, db);

  const std::string first_value(4096, 'a');
  const std::string failed_value(4096, 'b');
  const std::string later_value(4096, 'c');
  Status first_status;
  Status failed_status;
  Status later_status;
  WriteOptions sync;
  sync.sync = true;

  std::thread first([&] {
    first_status = db->Put(sync, "first", first_value);
  });
  if (!env.WaitForFirstWrite(std::chrono::seconds(5))) {
    env.ReleaseFirstWrite();
    first.join();
    ADD_FAILURE() << "first positioned write did not block";
    delete db;
    CleanDir(options.env, dbname);
    return;
  }

  std::thread failed([&] {
    failed_status = db->Put(WriteOptions(), "failed", failed_value);
  });
  if (!env.WaitForSecondWrite(std::chrono::seconds(5))) {
    env.ReleaseFirstWrite();
    first.join();
    failed.join();
    ADD_FAILURE() << "second positioned write did not overlap the first";
    delete db;
    CleanDir(options.env, dbname);
    return;
  }

  // The injected second call waits for this later reservation to enter
  // WriteAtV. It then leaves a one-byte partial record and fails while this
  // later disjoint range is allowed to finish first.
  std::thread later([&] {
    later_status = db->Put(WriteOptions(), "later", later_value);
  });
  const bool failed_out_of_order =
      env.WaitForFailedWrite(std::chrono::seconds(5));
  env.ReleaseFirstWrite();
  first.join();
  failed.join();
  later.join();

  ASSERT_TRUE(failed_out_of_order)
      << "middle positioned write did not fail before the first completed";
  EXPECT_GE(env.max_active_writes(), 3);
  EXPECT_TRUE(first_status.ok()) << first_status.ToString();
  EXPECT_TRUE(failed_status.IsIOError()) << failed_status.ToString();
  EXPECT_TRUE(later_status.IsIOError()) << later_status.ToString();

  std::string value;
  ASSERT_TRUE(db->Get(ReadOptions(), "first", &value).ok());
  EXPECT_EQ(first_value, value);
  EXPECT_TRUE(db->Get(ReadOptions(), "failed", &value).IsNotFound());
  EXPECT_TRUE(db->Get(ReadOptions(), "later", &value).IsNotFound());

  std::string pipeline;
  ASSERT_TRUE(db->GetProperty("leveldb.blobdb.write-pipeline", &pipeline));
  EXPECT_NE(std::string::npos, pipeline.find("requests=3")) << pipeline;
  EXPECT_NE(std::string::npos, pipeline.find("max_vlog_inflight=3"))
      << pipeline;
  EXPECT_NE(std::string::npos, pipeline.find("vlog_syncs=1")) << pipeline;

  delete db;
  db = nullptr;

  // Finish() must remove both the partial failed range and the successfully
  // completed but unpublishable later tail before recovery scans the file.
  uint64_t file_size = 0;
  ASSERT_TRUE(options.env->GetFileSize(VLogFileName(dbname, 3), &file_size).ok());
  EXPECT_EQ(SizeOf("first", first_value), file_size);

  ASSERT_TRUE(BlobDB::Open(options, dbname, &db).ok());
  ASSERT_TRUE(db->Get(ReadOptions(), "first", &value).ok());
  EXPECT_EQ(first_value, value);
  EXPECT_TRUE(db->Get(ReadOptions(), "failed", &value).IsNotFound());
  EXPECT_TRUE(db->Get(ReadOptions(), "later", &value).IsNotFound());

  delete db;
  CleanDir(options.env, dbname);
}

TEST(VLOG_TEST, BlockedAppendDoesNotBlockReadOnlyVLogGet) {
  BlockingAppendVLogEnv env(Env::Default());
  Options options;
  options.env = &env;
  options.create_if_missing = true;
  options.blob_value_size_threshold = 1;
  options.blob_max_file_size = 1024;
  options.blob_gc_interval = 0;
  options.blob_background_read_threads = 0;
  const std::string dbname("testdb-append-read-concurrency");
  CleanDir(options.env, dbname);

  BlobDB* db = nullptr;
  ASSERT_TRUE(BlobDB::Open(options, dbname, &db).ok());
  ASSERT_NE(nullptr, db);

  // An oversized first value rolls its file, so reads of `old-key` use a
  // read-only vlog while the next write appends to the new writable file.
  const std::string old_value(2048, 'o');
  ASSERT_TRUE(db->Put(WriteOptions(), "old-key", old_value).ok());

  env.BlockNextAppendV();
  Status write_status;
  std::thread writer([&] {
    write_status = db->Put(WriteOptions(), "new-key", std::string(512, 'n'));
  });
  const bool append_blocked =
      env.WaitForBlockedAppendV(std::chrono::seconds(5));
  if (!append_blocked) {
    env.ReleaseAppendV();
    writer.join();
    ADD_FAILURE() << "timed out waiting for the injected AppendV block";
    delete db;
    CleanDir(options.env, dbname);
    return;
  }

  Status read_status;
  std::string actual;
  std::mutex read_mu;
  std::condition_variable read_cv;
  bool read_done = false;
  std::thread reader([&] {
    read_status = db->Get(ReadOptions(), "old-key", &actual);
    {
      std::lock_guard<std::mutex> lock(read_mu);
      read_done = true;
    }
    read_cv.notify_one();
  });

  bool read_completed_while_append_blocked = false;
  {
    std::unique_lock<std::mutex> lock(read_mu);
    read_completed_while_append_blocked = read_cv.wait_for(
        lock, std::chrono::seconds(5), [&] { return read_done; });
  }

  // Always unblock and join before asserting, so a regression fails without
  // stranding a thread inside the fault-injection wrapper.
  env.ReleaseAppendV();
  writer.join();
  reader.join();

  EXPECT_TRUE(read_completed_while_append_blocked);
  EXPECT_TRUE(read_status.ok()) << read_status.ToString();
  EXPECT_EQ(old_value, actual);
  EXPECT_TRUE(write_status.ok()) << write_status.ToString();

  delete db;
  CleanDir(options.env, dbname);
}

TEST(VLOG_TEST, WriteErrorsDoNotPublishHandles) {
  ErrorInjectingVLogEnv env(Env::Default());
  Options options;
  options.env = &env;
  options.create_if_missing = true;
  options.blob_value_size_threshold = 1;
  options.blob_background_read_threads = 0;
  const std::string dbname("testdb-vlog-write-errors");
  CleanDir(options.env, dbname);

  BlobDB* db = nullptr;
  ASSERT_TRUE(BlobDB::Open(options, dbname, &db).ok());
  ASSERT_NE(nullptr, db);
  ASSERT_TRUE(db->Put(WriteOptions(), "committed", "value").ok());

  env.fail_append.store(true, std::memory_order_release);
  Status s = db->Put(WriteOptions(), "append-failed", "orphan");
  ASSERT_TRUE(s.IsIOError()) << s.ToString();
  env.fail_append.store(false, std::memory_order_release);
  Status later = db->Put(WriteOptions(), "after-append-failure", "orphan");
  EXPECT_TRUE(later.IsIOError()) << later.ToString();

  std::string value;
  ASSERT_TRUE(db->Get(ReadOptions(), "append-failed", &value).IsNotFound());
  ASSERT_TRUE(db->Get(ReadOptions(), "committed", &value).ok());
  ASSERT_EQ("value", value);
  delete db;
  CleanDir(options.env, dbname);

  ASSERT_TRUE(BlobDB::Open(options, dbname, &db).ok());
  WriteOptions sync_options;
  sync_options.sync = true;
  env.fail_sync.store(true, std::memory_order_release);
  s = db->Put(sync_options, "sync-failed", "orphan");
  ASSERT_TRUE(s.IsIOError()) << s.ToString();
  env.fail_sync.store(false, std::memory_order_release);
  later = db->Put(WriteOptions(), "after-sync-failure", "orphan");
  EXPECT_TRUE(later.IsIOError()) << later.ToString();
  ASSERT_TRUE(db->Get(ReadOptions(), "sync-failed", &value).IsNotFound());
  delete db;
  CleanDir(options.env, dbname);
}

TEST(VLOG_TEST, SyncSmallWriteFlushesEarlierNonsyncBlob) {
  Options options;
  options.create_if_missing = true;
  options.blob_value_size_threshold = 1024;
  options.blob_max_file_size = 1 << 20;
  options.blob_gc_interval = 0;
  options.blob_background_read_threads = 0;
  const std::string dbname("testdb-cross-log-sync-barrier");
  CleanDir(options.env, dbname);

  BlobDB* raw_db = nullptr;
  ASSERT_TRUE(BlobDB::Open(options, dbname, &raw_db).ok());
  std::unique_ptr<BlobDB> db(raw_db);

  const std::string blob_value(256 << 10, 'b');
  WriteOptions nonsync;
  nonsync.sync = false;
  ASSERT_TRUE(db->Put(nonsync, "earlier-blob", blob_value).ok());

  // This value stays in the LSM. Its sync barrier must still make the earlier
  // value-log append durable before the corresponding LSM WAL is synced.
  WriteOptions sync;
  sync.sync = true;
  ASSERT_TRUE(db->Put(sync, "barrier", "small").ok());

  db.reset();
  ASSERT_TRUE(BlobDB::Open(options, dbname, &raw_db).ok());
  db.reset(raw_db);

  std::string value;
  ASSERT_TRUE(db->Get(ReadOptions(), "earlier-blob", &value).ok());
  EXPECT_EQ(blob_value, value);
  ASSERT_TRUE(db->Get(ReadOptions(), "barrier", &value).ok());
  EXPECT_EQ("small", value);

  db.reset();
  CleanDir(options.env, dbname);
}

TEST(VLOG_TEST, ConcurrentSyncAndNonsyncWritesAcrossRollover) {
  Options options;
  options.create_if_missing = true;
  options.blob_value_size_threshold = 32;
  options.blob_max_file_size = 8 << 10;
  options.blob_gc_interval = 0;
  options.blob_background_read_threads = 0;
  const std::string dbname("testdb-concurrent-sync-rollover");
  CleanDir(options.env, dbname);

  BlobDB* raw_db = nullptr;
  ASSERT_TRUE(BlobDB::Open(options, dbname, &raw_db).ok());
  std::unique_ptr<BlobDB> db(raw_db);

  constexpr int kWriters = 4;
  constexpr int kWritesPerWriter = 64;
  constexpr size_t kValueSize = 2048;
  std::atomic<int> ready{0};
  std::atomic<bool> start{false};
  std::vector<Status> writer_status(kWriters);
  std::vector<std::thread> writers;
  writers.reserve(kWriters);

  for (int writer = 0; writer < kWriters; ++writer) {
    writers.emplace_back([&, writer] {
      ready.fetch_add(1, std::memory_order_release);
      while (!start.load(std::memory_order_acquire)) {
        std::this_thread::yield();
      }
      for (int index = 0; index < kWritesPerWriter; ++index) {
        const std::string key = "writer-" + std::to_string(writer) + "-" +
                                std::to_string(index);
        const std::string value(
            kValueSize,
            static_cast<char>('a' + (writer * kWritesPerWriter + index) % 26));
        WriteOptions write_options;
        write_options.sync = ((writer + index) & 1) == 0;
        writer_status[writer] = db->Put(write_options, key, value);
        if (!writer_status[writer].ok()) {
          break;
        }
      }
    });
  }

  while (ready.load(std::memory_order_acquire) != kWriters) {
    std::this_thread::yield();
  }
  start.store(true, std::memory_order_release);
  for (std::thread& writer : writers) {
    writer.join();
  }
  for (const Status& status : writer_status) {
    ASSERT_TRUE(status.ok()) << status.ToString();
  }

  auto verify_all = [&](BlobDB* database) {
    for (int writer = 0; writer < kWriters; ++writer) {
      for (int index = 0; index < kWritesPerWriter; ++index) {
        const std::string key = "writer-" + std::to_string(writer) + "-" +
                                std::to_string(index);
        const std::string expected(
            kValueSize,
            static_cast<char>('a' + (writer * kWritesPerWriter + index) % 26));
        std::string actual;
        const Status status = database->Get(ReadOptions(), key, &actual);
        ASSERT_TRUE(status.ok()) << key << ": " << status.ToString();
        EXPECT_EQ(expected, actual) << key;
      }
    }
  };

  verify_all(db.get());
  db.reset();
  ASSERT_TRUE(BlobDB::Open(options, dbname, &raw_db).ok());
  db.reset(raw_db);
  verify_all(db.get());

  db.reset();
  CleanDir(options.env, dbname);
}
