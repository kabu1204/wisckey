// Copyright (c) 2023. Chengye YU <yuchengye2013 AT outlook.com>
// SPDX-License-Identifier: BSD-3-Clause

#include "db/blob_vlog_impl.h"
#include "db/filename.h"
#include <atomic>
#include <random>
#include <thread>
#include <unordered_map>

#include "leveldb/env.h"

#include "table/format.h"
#include "util/sync_point.h"

#include "gtest/gtest.h"

using namespace leveldb;

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
  ASSERT_TRUE(s.ok());

  s = db->Get(ReadOptions(), "key1", &value);
  ASSERT_TRUE(s.IsNotFound());

  db->Put(WriteOptions(), "key0", "val0");
  s = db->Write(WriteOptions(), &wb, &cb);
  s = db->Get(ReadOptions(), "key1", &value);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(value, "val1");

  delete db;
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

  Slice begin("key" + std::to_string(0));
  Slice end("key" + std::to_string(num_ondisk_batches * per_batch - 1));
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

  Slice begin("key" + std::to_string(0));
  Slice end("key" + std::to_string(num_ondisk_entries - 1));
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
    Slice key("k0" + std::to_string(i + 7));
    Slice val("value0" + std::to_string(i + 7));
    v->Put(WriteOptions(), key, val, &handle);
    ASSERT_EQ(handle, ValueHandle(3, 0, size, SizeOf(key, val)));
    size += SizeOf(key, val);
    num_entries++;
  }

  delete v;
  ValueLogImpl::Open(options, dbname, dbwrapper, &v);

  size = 0;
  for (int i = 1; i <= num_entries; i++) {
    Slice key("k1" + std::to_string(i));
    Slice val("value1" + std::to_string(i));
    v->Put(WriteOptions(), key, val, &handle);
    ASSERT_EQ(handle, ValueHandle(18, 0, size, SizeOf(key, val)));
    size += SizeOf(key, val);
  }

  size = 0;
  for (int i = 1; i <= num_entries; i++) {
    Slice key("k1" + std::to_string(i));
    Slice val("value1" + std::to_string(i));
    s = v->Get(ReadOptions(), ValueHandle(18, 0, size, SizeOf(key, val)),
               &value);
    ASSERT_TRUE(s.ok());
    ASSERT_EQ(Slice(value), val);
    size += SizeOf(key, val);
  }

  size = 0;
  for (int i = 1; i <= 1000000; i++) {
    Slice key("k1" + std::to_string(i));
    Slice val("value1" + std::to_string(i));
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

  BlobDB* db;
  BlobDB::Open(options, dbname, &db);
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
            ASSERT_TRUE(s.ok());
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
            ASSERT_TRUE(s.ok());
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