// Copyright 2019 The Chubao Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

#include <gtest/gtest.h>

#include "base/fs_util.h"
#include "../helper/helper_util.h"

#include "db/db.h"
#include "db/mass_tree_impl/manager_impl.h"
#include "db/mass_tree_impl/db_impl.h"

int main(int argc, char* argv[]) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

using namespace chubaodb::ds::db;

namespace chubaodb {
namespace ds {
namespace storage {

static void CheckDB(MasstreeDBImpl* db, const std::map<std::string, std::string>& def_kvs,
             const std::map<std::string, std::string>& txn_kvs) {
    // test get
    for (const auto& kv : def_kvs) {
        std::string value;
        auto s = db->Get(CFType::kData, kv.first, value);
        ASSERT_TRUE(s.ok()) << s.ToString();
        ASSERT_EQ(value, kv.second);
    }
    for (const auto& kv : txn_kvs) {
        std::string value;
        auto s = db->Get(CFType::kTxn, kv.first, value);
        ASSERT_TRUE(s.ok()) << s.ToString();
        ASSERT_EQ(value, kv.second);
    }

    // test iterator
    auto iter = db->NewIterator(db->GetStartKey(), db->GetEndKey());
    for (const auto& kv : def_kvs) {
        ASSERT_TRUE(iter->Valid());
        ASSERT_TRUE(iter->status().ok()) << iter->status().ToString();
        ASSERT_EQ(iter->Key(), kv.first);
        ASSERT_EQ(iter->KeySize(), kv.first.size());
        ASSERT_EQ(iter->Value(), kv.second);
        ASSERT_EQ(iter->ValueSize(), kv.second.size());
        iter->Next();
    }
    ASSERT_FALSE(iter->Valid());
    ASSERT_TRUE(iter->status().ok()) << iter->status().ToString();

    std::unique_ptr<Iterator> default_iter, txn_iter;
    auto s = db->NewIterators(db->GetStartKey(), db->GetEndKey(), default_iter, txn_iter);
    ASSERT_TRUE(s.ok()) << s.ToString();
    for (const auto& kv : def_kvs) {
        ASSERT_TRUE(default_iter->Valid());
        ASSERT_TRUE(default_iter->status().ok()) << default_iter->status().ToString();
        ASSERT_EQ(default_iter->Key(), kv.first);
        ASSERT_EQ(default_iter->KeySize(), kv.first.size());
        ASSERT_EQ(default_iter->Value(), kv.second);
        ASSERT_EQ(default_iter->ValueSize(), kv.second.size());
        default_iter->Next();
    }
    ASSERT_FALSE(default_iter->Valid());
    ASSERT_TRUE(default_iter->status().ok()) << default_iter->status().ToString();
    for (const auto& kv : txn_kvs) {
        ASSERT_TRUE(txn_iter->Valid());
        ASSERT_TRUE(txn_iter->status().ok()) << txn_iter->status().ToString();
        ASSERT_EQ(txn_iter->Key(), kv.first);
        ASSERT_EQ(txn_iter->KeySize(), kv.first.size());
        ASSERT_EQ(txn_iter->Value(), kv.second);
        ASSERT_EQ(txn_iter->ValueSize(), kv.second.size());
        txn_iter->Next();
    }
    ASSERT_FALSE(txn_iter->Valid());
    ASSERT_TRUE(txn_iter->status().ok()) << txn_iter->status().ToString();
}

class DBTest: public ::testing::Test {
protected:
    static const size_t kCheckpointThresold = 4096;
    static const size_t kCheckpointMaxHistory = 2;

    DBTest() = default;

    void makeTestOptions(MasstreeOptions& ops) {
        ops.data_path = data_path_;
        ops.rcu_interval_ms = 1000;
        ops.checkpoint_opt.disabled = false;
        ops.checkpoint_opt.checksum = true;
        ops.checkpoint_opt.work_threads = 1;
        ops.checkpoint_opt.threshold_bytes = kCheckpointThresold;
        ops.checkpoint_opt.max_history = kCheckpointMaxHistory;
    }

    void SetUp() override {
        char path[] = "/tmp/chubaodb_ds_db_test_XXXXXX";
        char *tmp = mkdtemp(path);
        ASSERT_TRUE(tmp != NULL);
        data_path_ = tmp;

        MasstreeOptions ops;
        makeTestOptions(ops);
        manager_ = new MasstreeDBManager(ops);
        auto s = manager_->Init();
        ASSERT_TRUE(s.ok()) << s.ToString();

        OpenDB();
    }

    void TearDown() override {
        delete db_;
        delete manager_;
        if (!data_path_.empty()) {
            chubaodb::RemoveDirAll(data_path_.c_str());
        }
    }

    void OpenDB() {
        std::unique_ptr<DB> db;
        auto s = manager_->CreateDB(range_id_, start_key_, end_key_, db);
        ASSERT_TRUE(s.ok()) << s.ToString();
        db_ = dynamic_cast<MasstreeDBImpl*>(db.release());
    }

    void ReOpenDB() {
        auto s = db_->Close();
        ASSERT_TRUE(s.ok()) << s.ToString();
        delete db_;

        delete manager_;
        MasstreeOptions ops;
        makeTestOptions(ops);
        manager_ = new MasstreeDBManager(ops);
        s = manager_->Init();
        ASSERT_TRUE(s.ok()) << s.ToString();

        OpenDB();
    }

    void Check(const std::map<std::string, std::string>& def_kvs,
                 const std::map<std::string, std::string>& txn_kvs) {
        CheckDB(db_, def_kvs, txn_kvs);
    }

protected:
    const uint64_t range_id_ = 1;
    const std::string start_key_ = "\x00";
    const std::string end_key_ = "\xff ";

    std::string data_path_;
    MasstreeDBManager* manager_ = nullptr;
    MasstreeDBImpl* db_ = nullptr;
    uint64_t index_ = 0;
};

} // namespace storage
} // namespace ds
} // namespace chubaodb


namespace {

using namespace chubaodb;
using namespace chubaodb::ds::storage;

TEST_F(DBTest, InitalState) {
    ASSERT_EQ(db_->PersistApplied(), 0U);
    // no data
    auto iter = db_->NewIterator(start_key_, end_key_);
    ASSERT_FALSE(iter->Valid());
    ASSERT_TRUE(iter->status().ok()) << iter->status().ToString();

    std::unique_ptr<Iterator> default_iter, txn_iter;
    auto s = db_->NewIterators(start_key_, end_key_, default_iter, txn_iter);
    ASSERT_TRUE(s.ok()) << s.ToString();
    ASSERT_FALSE(default_iter->Valid());
    ASSERT_TRUE(default_iter->status().ok()) << default_iter->status().ToString();
    ASSERT_FALSE(txn_iter->Valid());
    ASSERT_TRUE(txn_iter->status().ok()) << txn_iter->status().ToString();
}

TEST_F(DBTest, PutGetDelete) {
    db_->TEST_Disable_Checkpoint();

    // with cf
    for (int i = 0; i < 10; ++i) {
        auto cf = i % 2 == 0 ? CFType::kData : CFType::kTxn;
        for (int j = 0; j < 1000; ++j) {
            std::string key = chubaodb::randomString(32, 1024);
            std::string value = chubaodb::randomString(64);
            auto s = db_->Put(cf, key, value, ++index_);
            ASSERT_TRUE(s.ok()) << s.ToString();

            std::string actual_value;
            s = db_->Get(cf, key, actual_value);
            ASSERT_TRUE(s.ok()) << s.ToString();
            ASSERT_EQ(actual_value, value);

            s = db_->Delete(cf, key, ++index_);
            ASSERT_TRUE(s.ok()) << s.ToString();
            s = db_->Get(cf, key, actual_value);
            ASSERT_EQ(s.code(), Status::kNotFound);
        }
    }

    // no cf
    for (int j = 0; j < 1000; ++j) {
        std::string key = chubaodb::randomString(32, 1024);
        std::string value = chubaodb::randomString(64);
        auto s = db_->DB::Put(key, value, ++index_);
        ASSERT_TRUE(s.ok()) << s.ToString();

        std::string actual_value;
        s = db_->DB::Get(key, actual_value);
        ASSERT_TRUE(s.ok()) << s.ToString();
        ASSERT_EQ(actual_value, value);

        s = db_->DB::Delete(key, ++index_);
        ASSERT_TRUE(s.ok()) << s.ToString();
        s = db_->DB::Get(key, actual_value);
        ASSERT_EQ(s.code(), Status::kNotFound);
    }
}

TEST_F(DBTest, WriteBatch) {
    db_->TEST_Disable_Checkpoint();

    std::map<std::string, std::string> def_kvs;
    std::map<std::string, std::string> txn_kvs;
    std::map<std::string, std::string> *kvs = nullptr;
    auto batch = db_->NewWriteBatch();
    for (int j = 0; j < 1000; ++j) {
        auto cf = randomInt() % 2 == 0 ? CFType::kData : CFType::kTxn;
        kvs = cf == CFType::kData ? &def_kvs : &txn_kvs;
        auto put_op = randomInt() % 3 > 0;
        if (put_op) {
            std::string key = chubaodb::randomString(32, 1024);
            std::string value = chubaodb::randomString(64);
            batch->Put(cf, key, value);
            kvs->emplace(key, value);
        } else { // delete
            if (kvs->empty()) {
                continue;
            }
            auto it = kvs->begin();
            std::advance(it, randomInt() % kvs->size());
            batch->Delete(cf, it->first);
            kvs->erase(it);
        }
    }
    auto s = db_->Write(batch.get(), ++index_);
    ASSERT_TRUE(s.ok()) << s.ToString();

    std::cout << "default size: " << def_kvs.size() << ", txn size: " << txn_kvs.size() << std::endl;

    Check(def_kvs, txn_kvs);
}

TEST_F(DBTest, Checkpoint) {
    db_->TEST_Disable_Checkpoint();

    std::map<std::string, std::string> def_kvs;
    std::map<std::string, std::string> txn_kvs;
    std::map<std::string, std::string> *kvs = nullptr;
    for (int j = 0; j < 5000; ++j) {
        auto cf = randomInt() % 2 == 0 ? CFType::kData : CFType::kTxn;
        kvs = cf == CFType::kData ? &def_kvs : &txn_kvs;
        std::string key = chubaodb::randomString(32, 1024);
        std::string value = chubaodb::randomString(64);
//        std::string key = std::string("key-") + std::to_string(j);
//        std::string value = std::string("value-") + std::to_string(j);
        auto s = db_->Put(cf, key, value, ++index_);
        ASSERT_TRUE(s.ok()) << s.ToString();
        kvs->emplace(key, value);
        if (j % 1000 == 500) {
            auto s = db_->TEST_Run_Checkpoint();
            ASSERT_TRUE(s.ok()) << s.ToString();
            ASSERT_EQ(db_->PersistApplied(), index_);
        }
    }
    Check(def_kvs, txn_kvs);

    auto s = db_->TEST_Run_Checkpoint();
    ASSERT_TRUE(s.ok()) << s.ToString();
    ASSERT_EQ(db_->PersistApplied(), index_);

    Check(def_kvs, txn_kvs);

    ReOpenDB();
    ASSERT_EQ(db_->PersistApplied(), index_);
    Check(def_kvs, txn_kvs);
}

TEST_F(DBTest, Snapshot) {
    // put some data, to verify snapshot can clear db
    db_->Put(CFType::kData, "123", "456", ++index_);
    db_->Put(CFType::kTxn, "123", "456", ++index_);

    std::map<std::string, std::string> def_kvs;
    std::map<std::string, std::string> txn_kvs;
    std::map<std::string, std::string> *kvs = nullptr;
    std::vector<std::unique_ptr<WriteBatch>> batchs;
    batchs.push_back(db_->NewWriteBatch());
    for (int j = 0; j < 1000; ++j) {
        auto cf = randomInt() % 2 == 0 ? CFType::kData : CFType::kTxn;
        kvs = cf == CFType::kData ? &def_kvs : &txn_kvs;
        std::string key = chubaodb::randomString(32, 1024);
        std::string value = chubaodb::randomString(64);
        kvs->emplace(key, value);
        batchs.back()->Put(cf, key, value);
        if (j % 1000 == 500) {
            batchs.push_back(db_->NewWriteBatch());
        }
    }

    auto snap_index = randomInt();
    auto s = db_->ApplySnapshotStart(snap_index);
    ASSERT_TRUE(s.ok()) << s.ToString();
    ASSERT_EQ(db_->PersistApplied(), 0U);
    // should clear all db data
    Check({}, {});

    for (auto& batch : batchs) {
        s = db_->ApplySnapshotData(batch.get());
        ASSERT_TRUE(s.ok()) << s.ToString();
    }
    s = db_->ApplySnapshotFinish(snap_index);
    ASSERT_TRUE(s.ok()) << s.ToString();

    ASSERT_EQ(db_->PersistApplied(), static_cast<uint64_t>(snap_index));
    Check(def_kvs, txn_kvs);

    ReOpenDB();
    ASSERT_EQ(db_->PersistApplied(), static_cast<uint64_t>(snap_index));
    Check(def_kvs, txn_kvs);
}

TEST_F(DBTest, Split) {
    std::map<std::string, std::string> def_kvs;
    std::map<std::string, std::string> txn_kvs;
    std::map<std::string, std::string> *kvs = nullptr;
    std::string split_key;
    for (int j = 0; j < 2000; ++j) {
        char key[64] = {'\0'};
        snprintf(key, 64, "%010d", j);
        std::string value = chubaodb::randomString(64);

        auto s = db_->Put(CFType::kData, key, value, ++index_);
        ASSERT_TRUE(s.ok()) << s.ToString();
        s = db_->Put(CFType::kTxn, key, value, ++index_);
        ASSERT_TRUE(s.ok()) << s.ToString();

        if (j == 1000) {
            split_key = key;
        }
        if (j >= 1000) {
            def_kvs.emplace(key, value);
            txn_kvs.emplace(key, value);
        }
    }

    std::unique_ptr<DB> split_db;
    auto split_index = ++index_;
    auto s = db_->SplitDB(2, split_key, split_index, split_db);
    ASSERT_TRUE(s.ok()) << s.ToString();
    ASSERT_EQ(split_db->PersistApplied(), split_index);
    CheckDB(dynamic_cast<MasstreeDBImpl*>(split_db.get()), def_kvs, txn_kvs);

    // close and reopen split db
    s = split_db->Close();
    ASSERT_TRUE(s.ok()) << s.ToString();
    split_db.reset();
    s = manager_->CreateDB(2, split_key, end_key_, split_db);
    ASSERT_TRUE(s.ok()) << s.ToString();
    ASSERT_EQ(split_db->PersistApplied(), split_index);
    CheckDB(dynamic_cast<MasstreeDBImpl*>(split_db.get()), def_kvs, txn_kvs);
}

}

