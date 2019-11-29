// Copyright 2019 The ChubaoDB Authors.
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

#include "base/status.h"
#include "base/util.h"
#include "common/server_config.h"
#include "base/fs_util.h"
#include "server/run_status.h"
#include "storage/store.h"

#include "masstree-beta/config.h"
#include "masstree-beta/string.hh"

#include "helper/table.h"
#include "helper/mock/raft_mock.h"
#include "helper/mock/rpc_request_mock.h"
#include "helper/helper_util.h"

#include "helper/cpp_permission.h"
#include "range/range.h"
#include "server/range_tree.h"
#include "server/range_server.h"

#include <string>
#include <map>

int main(int argc, char *argv[]) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

using namespace chubaodb::test::helper;
using namespace chubaodb::test::mock;
using namespace chubaodb::ds;
using namespace chubaodb::ds::storage;

const static uint32_t default_range_id = 1;
std::unique_ptr<Table> ptr_table_account = CreateAccountTable();

class SelectFlowTest : public ::testing::Test {
protected:
    void SetUp() override {

        data_path_ = "/tmp/chubaodb_ds_store_test_";
        data_path_ += std::to_string(NowMilliSeconds());

        ds_config.engine_type = EngineType::kMassTree;
        ds_config.masstree_config.data_path = data_path_ + "/mass-data";
        ds_config.meta_path = data_path_ + "/meta-data";

        ds_config.range_config.recover_concurrency = 1;
        ds_config.logger_config.level = "debug" ;
        ds_config.logger_config.path= data_path_ + "/log";
        ds_config.logger_config.name= "ds_log.log" ;

        range_server_ = new server::RangeServer;

        context_ = new server::ContextServer;

        context_->node_id = 1;
        context_->range_server = range_server_;
        context_->raft_server = new RaftServerMock;
        context_->run_status = new server::RunStatus;

        range_server_->Init(context_);
    }

    void TearDown() override {
        if (context_->range_server) {
            context_->range_server->Stop();
            delete context_->range_server;
        }
        delete context_->raft_server;
        delete context_->run_status;
        delete context_;
        if (!data_path_.empty()) {
            RemoveDirAll(data_path_.c_str());
        }
    }

    Status testSelectFlow(const dspb::RangeRequest& req, dspb::RangeResponse& resp) {
        auto rpc = NewMockRPCRequest(req, dspb::kFuncRangeRequest);
        range_server_->DealTask(std::move(rpc.first));
        return rpc.second->Get(resp);
    }

    Status testKv(const dspb::RangeRequest& req, dspb::RangeResponse& resp) {
        auto rpc = NewMockRPCRequest(req, dspb::kFuncRangeRequest);
        range_server_->DealTask(std::move(rpc.first));
        return rpc.second->Get(resp);
    }

protected:
    server::ContextServer *context_;
    std::string data_path_;
    server::RangeServer *range_server_;
};

/**
 * Test Info.
 *
 *  create table account (
 *      id bigint,
 *      name varchar(255),
 *      balance bigint,
 *      primary key (id)
 *  );
 *
 * id is unique index
 * balance is non unique index
 *
 * ================================================
 * |   id   |       name        |     balance     |
 * ================================================
 * ...
 * | 101010 |    10:10:10       |     101010      |
 * | 111111 |    11:11:11       |     111111      |
 * ...
 * ================================================
 *
 * */

void genKvPrimaryKeyData(std::map<std::string, std::string > & mp)
{

    typedef uint64_t col_id_type;
    for (auto id = 0; id < 20; id++) {
        std::string key;
        std::string value;

        auto h = id % 24;
        auto mi = id % 60;
        auto s = id % 60;

        datatype::MyTime dt;
        dt.SetHour(h);
        dt.SetMinute(mi);
        dt.SetSecond(s);

        auto tmp_id = dt.ToNumberInt64();
        auto tmp_name =  dt.ToString();
        auto tmp_balance = tmp_id;

        // make row data
        std::map<col_id_type, std::string> row;
        row.emplace(1, std::to_string(tmp_id));
        row.emplace(2, tmp_name);
        row.emplace(3, std::to_string(tmp_balance));

        // make encode key
        EncodeKeyPrefix(&key, default_account_table_id);
        for ( auto & col : ptr_table_account->GetPKs()) {
            EncodePrimaryKey(&key, col,row.at(col.id()));
        }

        // make encode value
        for (auto & col : ptr_table_account->GetNonPkColumns()) {
            EncodeColumnValue(&value, col, row.at(col.id()));
        }

        mp.emplace(key, value);
    }
}

bool check_data(std::string & id, std::string & name, std::string & balance)
{
    bool flg = false;

    datatype::MyTime t;
    datatype::MyTime::StWarn st;

    bool ret_b  = t.FromString(name, st);
    if (!ret_b ) {
        t.SetZero();
        std::cerr << "Time.FromString(12:12:12) Failed."  << "line:" << __LINE__ << std::endl;
    }
    auto tmp_name = std::to_string(t.ToNumberInt64());

    if ( id == balance && id == tmp_name ) {
        flg = true;
    } else {
        flg = false;
        std::cerr << "id:" << id << "\tname:" << name << "\tbalance:" << balance << std::endl;
    }

    return flg;

}

bool check_row_value (const dspb::RowValue & v)
{
    size_t offset = 0;

    std::string ret_id;
    std::string ret_name;
    std::string ret_balance;

    std::string buf = v.fields();
    // return data sort is id,name,balance

    DecodeColumnValue(buf, offset, ptr_table_account->GetColumnInfo("id"), &ret_id);
    DecodeColumnValue(buf, offset, ptr_table_account->GetColumnInfo("name"), &ret_name);
    DecodeColumnValue(buf, offset, ptr_table_account->GetColumnInfo("balance"), &ret_balance);

    return check_data(ret_id, ret_name, ret_balance);
}

basepb::Range *getPrimaryKeyRange() {
    auto meta = new basepb::Range;

    meta->set_id(default_range_id);
    std::string start_key;
    std::string end_key;
    EncodeKeyPrefix(&start_key, default_account_table_id);
    EncodeKeyPrefix(&end_key, default_account_table_id+1);

    meta->set_start_key(start_key);
    meta->set_end_key(end_key);
    meta->mutable_range_epoch()->set_conf_ver(1);
    meta->mutable_range_epoch()->set_version(1);

    meta->set_table_id(default_account_table_id);

    auto peer = meta->add_peers();
    peer->set_id(1);
    peer->set_node_id(1);

    peer = meta->add_peers();
    peer->set_id(2);
    peer->set_node_id(2);

    auto pks = ptr_table_account->GetPKs();
    for (const auto& pk : pks) {
        auto p = meta->add_primary_keys();
        p->CopyFrom(pk);
    }

    return meta;
}

basepb::Range *genIndexKeyRange() {
    auto meta = new basepb::Range;

    meta->set_id(default_range_id+1);
    std::string start_key;
    std::string end_key;
    EncodeIndexKeyPrefix(&start_key, default_account_table_id);
    EncodeIndexKeyPrefix(&end_key, default_account_table_id+1);

    meta->set_start_key(start_key);
    meta->set_end_key(end_key);
    meta->mutable_range_epoch()->set_conf_ver(1);
    meta->mutable_range_epoch()->set_version(1);

    meta->set_table_id(default_account_table_id);

    auto peer = meta->add_peers();
    peer->set_id(1);
    peer->set_node_id(1);

    peer = meta->add_peers();
    peer->set_id(2);
    peer->set_node_id(2);

    auto pks = ptr_table_account->GetPKs();
    for (const auto& pk : pks) {
        auto p = meta->add_primary_keys();
        p->CopyFrom(pk);
    }

    return meta;
}

TEST_F(SelectFlowTest, SelectFlow_selection_table_read_logic)
{
    std::string start_key;
    std::string end_key;
    std::map<std::string, std::string> mp_kv;

    { // create table
        EncodeKeyPrefix(&start_key, default_account_table_id);
        EncodeKeyPrefix(&end_key, default_account_table_id+1);
    }


    { // begin create range
        dspb::SchRequest req;
        req.mutable_create_range()->set_allocated_range(getPrimaryKeyRange());

        // create range
        auto rpc = NewMockRPCRequest(req);
        range_server_->dispatchSchedule(rpc.first);
        ASSERT_FALSE(range_server_->ranges_.Empty());
        ASSERT_TRUE(range_server_->Find(default_range_id) != nullptr);

        // check meta
        std::vector<basepb::Range> metas;
        auto ret = range_server_->meta_store_->GetAllRange(&metas);
        ASSERT_TRUE(metas.size() == 1) << metas.size();

        // raft
        auto raft = static_cast<RaftMock *>(range_server_->ranges_.Find(1)->raft_.get());
        raft->SetLeaderTerm(1, 1);
        range_server_->ranges_.Find(1)->is_leader_ = true;

    }

    { // put data

        genKvPrimaryKeyData(mp_kv);

        for (auto kv : mp_kv) {
            // put data to db
            dspb::RangeRequest req;
            req.mutable_header()->set_range_id(default_range_id);
            req.mutable_header()->mutable_range_epoch()->set_conf_ver(1);
            req.mutable_header()->mutable_range_epoch()->set_version(1);
            req.mutable_kv_put()->set_key(kv.first);
            req.mutable_kv_put()->set_value(kv.second);

            dspb::RangeResponse resp;
            auto s = testKv(req, resp);
            ASSERT_TRUE(s.ok()) << s.ToString();
            ASSERT_FALSE(resp.header().has_error());
        }

    }

    // Time(12:12:12) == Time(12:12:12)
    // Time(12:12:12) != Time(12:12:12)
    // Time(12:12:12) < Time(12:12:12)
    // Time(12:12:12) <= Time(12:12:12)
    // Time(12:12:12) > Time(12:12:12)
    // Time(12:12:12) >= Time(12:12:12)


    // Time(12:12:12) == Time(12:12:12)
    {
        dspb::RangeRequest req;
        auto header = req.mutable_header();
        header->set_range_id(default_range_id);
        header->mutable_range_epoch()->set_conf_ver(1);
        header->mutable_range_epoch()->set_version(1);

        auto select_flow = req.mutable_select_flow();
        // add table read processor
        auto table_read_processor = select_flow->add_processors();
        table_read_processor->set_type(dspb::TABLE_READ_TYPE );
        auto table_read = table_read_processor-> mutable_table_read();

        for ( const auto & col : ptr_table_account->GetAllColumns()) {
            table_read->add_columns()->CopyFrom(ptr_table_account->GetColumnInfo(col.id()));
        }

        table_read->set_type(dspb::KEYS_RANGE_TYPE);
        table_read->mutable_range()->set_start_key(start_key);
        table_read->mutable_range()->set_end_key(end_key);
        table_read->set_desc(false);

        auto selection_processor = select_flow->add_processors();
        selection_processor->set_type(dspb::SELECTION_TYPE);
        auto selection = selection_processor->mutable_selection();

        dspb::Expr expr_equal_time_1;
        expr_equal_time_1.set_expr_type(dspb::EqualTime);

        auto expr_equal_time_column = expr_equal_time_1.add_child();
        expr_equal_time_column->set_expr_type(dspb::Const_Time);
        datatype::MyTime t;
        datatype::MyTime::StWarn st;
        bool ret_b = t.FromString( "12:12:12", st);
        if (!ret_b) {
            t.SetZero();
            std::cerr << "Time.FromString(12:12:12) Failed."  << "line:" << __LINE__ << std::endl;
        }

        expr_equal_time_column->set_value(t.ToString());
        auto expr_equal_time_value = expr_equal_time_1.add_child();
        expr_equal_time_value->set_expr_type(dspb::Const_Time);

        datatype::MyTime t2;
        datatype::MyTime::StWarn st2;
        bool ret_b2 = t2.FromString( "12:12:12", st2);
        if (!ret_b2) {
            t2.SetZero();
            std::cerr << "Time.FromString(12:12:12) Failed."  << "line:" << __LINE__ << std::endl;
        }

        expr_equal_time_value->set_value(t2.ToString());
        selection->add_filter()->CopyFrom(expr_equal_time_1);

        select_flow->add_output_offsets(0);
        select_flow->add_output_offsets(1);
        select_flow->add_output_offsets(2);

        select_flow->set_gather_trace(false);

        dspb::RangeResponse resp;
        auto s = testSelectFlow(req, resp);
        ASSERT_TRUE(s.ok()) << s.ToString();
        ASSERT_FALSE(resp.header().has_error());
        ASSERT_TRUE(resp.has_select_flow());
        ASSERT_TRUE(resp.select_flow().rows_size() == 20) << resp.select_flow().rows_size();

        {
            for (auto & row : resp.select_flow().rows()) {
                ASSERT_TRUE(mp_kv.find(row.key()) != mp_kv.end())
                        << "Cann't found key:" <<  row.key() << "\tmp_kv.size:" << mp_kv.size();
                ASSERT_TRUE(check_row_value(row.value())) << "check_row_value error:" << row.DebugString();
            }

        }
    }

    // Time(12:12:12) != Time(12:12:12)
    {
        dspb::RangeRequest req;
        auto header = req.mutable_header();
        header->set_range_id(default_range_id);
        header->mutable_range_epoch()->set_conf_ver(1);
        header->mutable_range_epoch()->set_version(1);

        auto select_flow = req.mutable_select_flow();
        // add table read processor
        auto table_read_processor = select_flow->add_processors();
        table_read_processor->set_type(dspb::TABLE_READ_TYPE );
        auto table_read = table_read_processor-> mutable_table_read();

        for ( const auto & col : ptr_table_account->GetAllColumns()) {
            table_read->add_columns()->CopyFrom(ptr_table_account->GetColumnInfo(col.id()));
        }

        table_read->set_type(dspb::KEYS_RANGE_TYPE);
        table_read->mutable_range()->set_start_key(start_key);
        table_read->mutable_range()->set_end_key(end_key);
        table_read->set_desc(false);

        auto selection_processor = select_flow->add_processors();
        selection_processor->set_type(dspb::SELECTION_TYPE);
        auto selection = selection_processor->mutable_selection();

        dspb::Expr expr_equal_time_1;
        expr_equal_time_1.set_expr_type(dspb::NotEqualTime);

        auto expr_equal_time_column = expr_equal_time_1.add_child();
        expr_equal_time_column->set_expr_type(dspb::Const_Time);
        datatype::MyTime t;
        datatype::MyTime::StWarn st;
        bool ret_b = t.FromString( "12:12:12", st);
        if (!ret_b) {
            t.SetZero();
            std::cerr << "Time.FromString(12:12:12) Failed."  << "line:" << __LINE__ << std::endl;
        }

        expr_equal_time_column->set_value(t.ToString());
        auto expr_equal_time_value = expr_equal_time_1.add_child();
        expr_equal_time_value->set_expr_type(dspb::Const_Time);

        datatype::MyTime t2;
        datatype::MyTime::StWarn st2;
        bool ret_b2 = t2.FromString( "12:12:12", st2);
        if (!ret_b2) {
            t2.SetZero();
            std::cerr << "Time.FromString(12:12:12) Failed."  << "line:" << __LINE__ << std::endl;
        }

        expr_equal_time_value->set_value(t2.ToString());
        selection->add_filter()->CopyFrom(expr_equal_time_1);

        select_flow->add_output_offsets(0);
        select_flow->add_output_offsets(1);
        select_flow->add_output_offsets(2);

        select_flow->set_gather_trace(false);

        dspb::RangeResponse resp;
        auto s = testSelectFlow(req, resp);
        ASSERT_TRUE(s.ok()) << s.ToString();
        ASSERT_FALSE(resp.header().has_error());
        ASSERT_TRUE(resp.has_select_flow());
        ASSERT_TRUE(resp.select_flow().rows_size() == 0) << resp.select_flow().rows_size();

        {
            for (auto & row : resp.select_flow().rows()) {
                ASSERT_TRUE(mp_kv.find(row.key()) != mp_kv.end())
                        << "Cann't found key:" <<  row.key() << "\tmp_kv.size:" << mp_kv.size();
                ASSERT_TRUE(check_row_value(row.value())) << "check_row_value error:" << row.DebugString();
            }

        }
    }

    // Time(12:12:12) <  Time(12:12:12)
    {
        dspb::RangeRequest req;
        auto header = req.mutable_header();
        header->set_range_id(default_range_id);
        header->mutable_range_epoch()->set_conf_ver(1);
        header->mutable_range_epoch()->set_version(1);

        auto select_flow = req.mutable_select_flow();
        // add table read processor
        auto table_read_processor = select_flow->add_processors();
        table_read_processor->set_type(dspb::TABLE_READ_TYPE );
        auto table_read = table_read_processor-> mutable_table_read();

        for ( const auto & col : ptr_table_account->GetAllColumns()) {
            table_read->add_columns()->CopyFrom(ptr_table_account->GetColumnInfo(col.id()));
        }

        table_read->set_type(dspb::KEYS_RANGE_TYPE);
        table_read->mutable_range()->set_start_key(start_key);
        table_read->mutable_range()->set_end_key(end_key);
        table_read->set_desc(false);

        auto selection_processor = select_flow->add_processors();
        selection_processor->set_type(dspb::SELECTION_TYPE);
        auto selection = selection_processor->mutable_selection();

        dspb::Expr expr_equal_time_1;
        expr_equal_time_1.set_expr_type(dspb::LessTime);

        auto expr_equal_time_column = expr_equal_time_1.add_child();
        expr_equal_time_column->set_expr_type(dspb::Const_Time);
        datatype::MyTime t;
        datatype::MyTime::StWarn st;
        bool ret_b = t.FromString( "12:12:12", st);
        if (!ret_b) {
            t.SetZero();
            std::cerr << "Time.FromString(12:12:12) Failed."  << "line:" << __LINE__ << std::endl;
        }

        expr_equal_time_column->set_value(t.ToString());
        auto expr_equal_time_value = expr_equal_time_1.add_child();
        expr_equal_time_value->set_expr_type(dspb::Const_Time);

        datatype::MyTime t2;
        datatype::MyTime::StWarn st2;
        bool ret_b2 = t2.FromString( "12:12:12", st2);
        if (!ret_b2) {
            t2.SetZero();
            std::cerr << "Time.FromString(12:12:12) Failed."  << "line:" << __LINE__ << std::endl;
        }

        expr_equal_time_value->set_value(t2.ToString());
        selection->add_filter()->CopyFrom(expr_equal_time_1);

        select_flow->add_output_offsets(0);
        select_flow->add_output_offsets(1);
        select_flow->add_output_offsets(2);

        select_flow->set_gather_trace(false);

        dspb::RangeResponse resp;
        auto s = testSelectFlow(req, resp);
        ASSERT_TRUE(s.ok()) << s.ToString();
        ASSERT_FALSE(resp.header().has_error());
        ASSERT_TRUE(resp.has_select_flow());
        ASSERT_TRUE(resp.select_flow().rows_size() == 0) << resp.select_flow().rows_size();

        {
            for (auto & row : resp.select_flow().rows()) {
                ASSERT_TRUE(mp_kv.find(row.key()) != mp_kv.end())
                        << "Cann't found key:" <<  row.key() << "\tmp_kv.size:" << mp_kv.size();
                ASSERT_TRUE(check_row_value(row.value())) << "check_row_value error:" << row.DebugString();
            }

        }
    }

    // Time(12:12:12) <=  Time(12:12:12)
    {
        dspb::RangeRequest req;
        auto header = req.mutable_header();
        header->set_range_id(default_range_id);
        header->mutable_range_epoch()->set_conf_ver(1);
        header->mutable_range_epoch()->set_version(1);

        auto select_flow = req.mutable_select_flow();
        // add table read processor
        auto table_read_processor = select_flow->add_processors();
        table_read_processor->set_type(dspb::TABLE_READ_TYPE );
        auto table_read = table_read_processor-> mutable_table_read();

        for ( const auto & col : ptr_table_account->GetAllColumns()) {

            table_read->add_columns()->CopyFrom(ptr_table_account->GetColumnInfo(col.id()));
        }

        table_read->set_type(dspb::KEYS_RANGE_TYPE);
        table_read->mutable_range()->set_start_key(start_key);
        table_read->mutable_range()->set_end_key(end_key);
        table_read->set_desc(false);

        auto selection_processor = select_flow->add_processors();
        selection_processor->set_type(dspb::SELECTION_TYPE);
        auto selection = selection_processor->mutable_selection();


        dspb::Expr expr_equal_time_1;
        expr_equal_time_1.set_expr_type(dspb::LessOrEqualTime);

        auto expr_equal_time_column = expr_equal_time_1.add_child();
        expr_equal_time_column->set_expr_type(dspb::Const_Time);
        datatype::MyTime dt;
        datatype::MyTime::StWarn st;
        bool ret_b = dt.FromString( "12:12:12", st);
        if (!ret_b) {
            dt.SetZero();
            std::cerr << "Time.FromString(12:12:12) Failed."  << "line:" << __LINE__ << std::endl;
        }

        expr_equal_time_column->set_value(dt.ToString());

        auto expr_equal_time_value = expr_equal_time_1.add_child();
        expr_equal_time_value->set_expr_type(dspb::Const_Time);

        datatype::MyTime t2;
        datatype::MyTime::StWarn st2;
        bool ret_b2 = t2.FromString("12:12:12", st2);
        if (!ret_b2) {
            t2.SetZero();
            std::cerr << "Time.FromString(12:12:12) Failed."  << "line:" << __LINE__ << std::endl;
        }

        expr_equal_time_value->set_value(t2.ToString());
        selection->add_filter()->CopyFrom(expr_equal_time_1);

        select_flow->add_output_offsets(0);
        select_flow->add_output_offsets(1);
        select_flow->add_output_offsets(2);

        select_flow->set_gather_trace(false);

        dspb::RangeResponse resp;
        auto s = testSelectFlow(req, resp);
        ASSERT_TRUE(s.ok()) << s.ToString();
        ASSERT_FALSE(resp.header().has_error());
        ASSERT_TRUE(resp.has_select_flow());
        ASSERT_TRUE(resp.select_flow().rows_size() == 20) << resp.select_flow().rows_size();

        {
            for (auto & row : resp.select_flow().rows()) {
                ASSERT_TRUE(mp_kv.find(row.key()) != mp_kv.end())
                        << "Cann't found key:" <<  row.key() << "\tmp_kv.size:" << mp_kv.size();
                ASSERT_TRUE(check_row_value(row.value())) << "check_row_value error:" << row.DebugString();
            }

        }
    }

    // Time(12:12:12) > Time(12:12:12)
    {
        dspb::RangeRequest req;
        auto header = req.mutable_header();
        header->set_range_id(default_range_id);
        header->mutable_range_epoch()->set_conf_ver(1);
        header->mutable_range_epoch()->set_version(1);

        auto select_flow = req.mutable_select_flow();
        // add table read processor
        auto table_read_processor = select_flow->add_processors();
        table_read_processor->set_type(dspb::TABLE_READ_TYPE );
        auto table_read = table_read_processor-> mutable_table_read();

        for ( const auto & col : ptr_table_account->GetAllColumns()) {
            table_read->add_columns()->CopyFrom(ptr_table_account->GetColumnInfo(col.id()));
        }

        table_read->set_type(dspb::KEYS_RANGE_TYPE);
        table_read->mutable_range()->set_start_key(start_key);
        table_read->mutable_range()->set_end_key(end_key);
        table_read->set_desc(false);

        auto selection_processor = select_flow->add_processors();
        selection_processor->set_type(dspb::SELECTION_TYPE);
        auto selection = selection_processor->mutable_selection();

        dspb::Expr expr_equal_time_1;
        expr_equal_time_1.set_expr_type(dspb::GreaterTime);

        auto expr_equal_time_column = expr_equal_time_1.add_child();
        expr_equal_time_column->set_expr_type(dspb::Const_Time);
        datatype::MyTime t;
        datatype::MyTime::StWarn st;
        bool ret_b = t.FromString( "2019-11-14 18:30:00", st);
        if (!ret_b) {
            t.SetZero();
            std::cerr << "Time.FromString(12:12:12) Failed."  << "line:" << __LINE__ << std::endl;
        }

        expr_equal_time_column->set_value(t.ToString());

        auto expr_equal_time_value = expr_equal_time_1.add_child();
        expr_equal_time_value->set_expr_type(dspb::Const_Time);

        datatype::MyTime t2;
        datatype::MyTime::StWarn st2;
        bool ret_b2 = t2.FromString( "2019-11-14 18:30:00", st2);
        if (!ret_b2) {
            t2.SetZero();
            std::cerr << "Time.FromString(12:12:12) Failed."  << "line:" << __LINE__ << std::endl;
        }

        expr_equal_time_value->set_value(t2.ToString());
        selection->add_filter()->CopyFrom(expr_equal_time_1);

        select_flow->add_output_offsets(0);
        select_flow->add_output_offsets(1);
        select_flow->add_output_offsets(2);

        select_flow->set_gather_trace(false);

        dspb::RangeResponse resp;
        auto s = testSelectFlow(req, resp);
        ASSERT_TRUE(s.ok()) << s.ToString();
        ASSERT_FALSE(resp.header().has_error());
        ASSERT_TRUE(resp.has_select_flow());
        ASSERT_TRUE(resp.select_flow().rows_size() == 0) << resp.select_flow().rows_size();

        {
            for (auto & row : resp.select_flow().rows()) {
                ASSERT_TRUE(mp_kv.find(row.key()) != mp_kv.end())
                        << "Cann't found key:" <<  row.key() << "\tmp_kv.size:" << mp_kv.size();
                ASSERT_TRUE(check_row_value(row.value())) << "check_row_value error:" << row.DebugString();
            }

        }
    }

    // Time(12:12:12) >= Time(12:12:12)
    {
        dspb::RangeRequest req;
        auto header = req.mutable_header();
        header->set_range_id(default_range_id);
        header->mutable_range_epoch()->set_conf_ver(1);
        header->mutable_range_epoch()->set_version(1);

        auto select_flow = req.mutable_select_flow();
        // add table read processor
        auto table_read_processor = select_flow->add_processors();
        table_read_processor->set_type(dspb::TABLE_READ_TYPE );
        auto table_read = table_read_processor-> mutable_table_read();

        for ( const auto & col : ptr_table_account->GetAllColumns()) {
            table_read->add_columns()->CopyFrom(ptr_table_account->GetColumnInfo(col.id()));
        }

        table_read->set_type(dspb::KEYS_RANGE_TYPE);
        table_read->mutable_range()->set_start_key(start_key);
        table_read->mutable_range()->set_end_key(end_key);
        table_read->set_desc(false);

        auto selection_processor = select_flow->add_processors();
        selection_processor->set_type(dspb::SELECTION_TYPE);
        auto selection = selection_processor->mutable_selection();


        dspb::Expr expr_equal_time_1;
        expr_equal_time_1.set_expr_type(dspb::GreaterOrEqualTime);

        auto expr_equal_time_column = expr_equal_time_1.add_child();
        expr_equal_time_column->set_expr_type(dspb::Const_Time);
        datatype::MyTime dt;
        datatype::MyTime::StWarn st;
        bool ret_b = dt.FromString( "12:12:12", st);
        if (!ret_b) {
            dt.SetZero();
            std::cerr << "Time.FromString(12:12:12) Failed."  << "line:" << __LINE__ << std::endl;
        }

        expr_equal_time_column->set_value(dt.ToString());

        auto expr_equal_time_value = expr_equal_time_1.add_child();
        expr_equal_time_value->set_expr_type(dspb::Const_Time);

        datatype::MyTime t2;
        datatype::MyTime::StWarn st2;
        bool ret_b2 = t2.FromString( "12:12:12", st2);
        if (!ret_b2) {
            t2.SetZero();
            std::cerr << "Time.FromString(12:12:12) Failed."  << "line:" << __LINE__ << std::endl;
        }

        expr_equal_time_value->set_value(t2.ToString());

        selection->add_filter()->CopyFrom(expr_equal_time_1);

        select_flow->add_output_offsets(0);
        select_flow->add_output_offsets(1);
        select_flow->add_output_offsets(2);

        select_flow->set_gather_trace(false);

        dspb::RangeResponse resp;
        auto s = testSelectFlow(req, resp);
        ASSERT_TRUE(s.ok()) << s.ToString();
        ASSERT_FALSE(resp.header().has_error());
        ASSERT_TRUE(resp.has_select_flow());
        ASSERT_TRUE(resp.select_flow().rows_size() == 20) << resp.select_flow().rows_size();

        {
            for (auto & row : resp.select_flow().rows()) {
                ASSERT_TRUE(mp_kv.find(row.key()) != mp_kv.end())
                        << "Cann't found key:" <<  row.key() << "\tmp_kv.size:" << mp_kv.size();
                ASSERT_TRUE(check_row_value(row.value())) << "check_row_value error:" << row.DebugString();
            }
        }
    }
}

TEST_F(SelectFlowTest, SelectFlow_selection_table_read_cast_time)
{
    std::string start_key;
    std::string end_key;
    std::map<std::string, std::string> mp_kv;

    { // create table
        EncodeKeyPrefix(&start_key, default_account_table_id);
        EncodeKeyPrefix(&end_key, default_account_table_id+1);
    }


    { // begin create range
        dspb::SchRequest req;
        req.mutable_create_range()->set_allocated_range(getPrimaryKeyRange());

        // create range
        auto rpc = NewMockRPCRequest(req);
        range_server_->dispatchSchedule(rpc.first);
        ASSERT_FALSE(range_server_->ranges_.Empty());
        ASSERT_TRUE(range_server_->Find(default_range_id) != nullptr);

        // check meta
        std::vector<basepb::Range> metas;
        auto ret = range_server_->meta_store_->GetAllRange(&metas);
        ASSERT_TRUE(metas.size() == 1) << metas.size();

        // raft
        auto raft = static_cast<RaftMock *>(range_server_->ranges_.Find(1)->raft_.get());
        raft->SetLeaderTerm(1, 1);
        range_server_->ranges_.Find(1)->is_leader_ = true;

    }

    { // put data

        genKvPrimaryKeyData(mp_kv);

        for (auto kv : mp_kv) {
            // put data to db
            dspb::RangeRequest req;
            req.mutable_header()->set_range_id(default_range_id);
            req.mutable_header()->mutable_range_epoch()->set_conf_ver(1);
            req.mutable_header()->mutable_range_epoch()->set_version(1);
            req.mutable_kv_put()->set_key(kv.first);
            req.mutable_kv_put()->set_value(kv.second);

            dspb::RangeResponse resp;
            auto s = testKv(req, resp);
            ASSERT_TRUE(s.ok()) << s.ToString();
            ASSERT_FALSE(resp.header().has_error());
        }

    }

    //=================================================================================
    //* CastIntToTime(id) > Time(10:10:10)
    //* id > CastTimeToInt(Time(10:10:10))

    //* CastStringToTime(name) > Time(10:10:10)
    //* name > CastTimeToString(Time(10:10:10))

    //* CastIntToTime(id) > CastRealToTime(101010.1010)
    //* CastIntToReal(id) > CastTimeToReal(Time(10:10:10))

    //* CastIntToTime(id) > CastDecimalToTime(Decimal(101010.101010))
    //* CastIntToDecimal(id) > CastTimeToDecimal(Time(10:10:10))

    //* CastIntToTime(id) > CastDateToTime(DateTime(2019-11-20 10:10:10))
    //* CastTimeToDate(CastIntToTime(id)) > CastTimeToDate(Time(10:10:10))

    //* CastIntToTime(id) > CastTimeToTime(Time(10:10:10))
    //=================================================================================

    //* CastIntToTime(id) > Time(10:10:10)
    {
        dspb::RangeRequest req;
        auto header = req.mutable_header();
        header->set_range_id(default_range_id);
        header->mutable_range_epoch()->set_conf_ver(1);
        header->mutable_range_epoch()->set_version(1);

        auto select_flow = req.mutable_select_flow();
        // add table read processor
        auto table_read_processor = select_flow->add_processors();
        table_read_processor->set_type(dspb::TABLE_READ_TYPE );
        auto table_read = table_read_processor-> mutable_table_read();

        for ( const auto & col : ptr_table_account->GetAllColumns()) {
            table_read->add_columns()->CopyFrom(ptr_table_account->GetColumnInfo(col.id()));
        }

        table_read->set_type(dspb::KEYS_RANGE_TYPE);
        table_read->mutable_range()->set_start_key(start_key);
        table_read->mutable_range()->set_end_key(end_key);
        table_read->set_desc(false);

        auto selection_processor = select_flow->add_processors();
        selection_processor->set_type(dspb::SELECTION_TYPE);
        auto selection = selection_processor->mutable_selection();

        dspb::Expr expr_greater_time_1;
        expr_greater_time_1.set_expr_type(dspb::GreaterTime);

        auto expr_cast_int_to_time = expr_greater_time_1.add_child();
        expr_cast_int_to_time->set_expr_type(dspb::CastIntToTime);

        auto expr_time_column = expr_cast_int_to_time->add_child();
        expr_time_column->set_expr_type(dspb::Column);
        expr_time_column->mutable_column()->CopyFrom(ptr_table_account->GetColumnInfo("id"));

        auto expr_time_value = expr_greater_time_1.add_child();
        expr_time_value->set_expr_type(dspb::Const_Time);

        datatype::MyTime t;
        datatype::MyTime::StWarn st;
        bool ret_b = t.FromString( "10:10:10", st);
        if (!ret_b) {
            t.SetZero();
        }

        expr_time_value->set_value(t.ToString());
        selection->add_filter()->CopyFrom(expr_greater_time_1);

        select_flow->add_output_offsets(0);
        select_flow->add_output_offsets(1);
        select_flow->add_output_offsets(2);

        select_flow->set_gather_trace(false);

        dspb::RangeResponse resp;
        auto s = testSelectFlow(req, resp);
        ASSERT_TRUE(s.ok()) << s.ToString();
        ASSERT_FALSE(resp.header().has_error());
        ASSERT_TRUE(resp.has_select_flow());
        ASSERT_TRUE(resp.select_flow().rows_size() == 9) << resp.select_flow().rows_size();

        {
            for (auto & row : resp.select_flow().rows()) {
                ASSERT_TRUE(mp_kv.find(row.key()) != mp_kv.end())
                        << "Cann't found key:" <<  row.key() << "\tmp_kv.size:" << mp_kv.size();
                ASSERT_TRUE(check_row_value(row.value())) << "check_row_value error:" << row.DebugString();
            }
        }
    }

    //* id > CastTimeToInt(Time(10:10:10))
    {
        dspb::RangeRequest req;
        auto header = req.mutable_header();
        header->set_range_id(default_range_id);
        header->mutable_range_epoch()->set_conf_ver(1);
        header->mutable_range_epoch()->set_version(1);

        auto select_flow = req.mutable_select_flow();
        // add table read processor
        auto table_read_processor = select_flow->add_processors();
        table_read_processor->set_type(dspb::TABLE_READ_TYPE );
        auto table_read = table_read_processor-> mutable_table_read();

        for ( const auto & col : ptr_table_account->GetAllColumns()) {
            table_read->add_columns()->CopyFrom(ptr_table_account->GetColumnInfo(col.id()));
        }

        table_read->set_type(dspb::KEYS_RANGE_TYPE);
        table_read->mutable_range()->set_start_key(start_key);
        table_read->mutable_range()->set_end_key(end_key);
        table_read->set_desc(false);

        auto selection_processor = select_flow->add_processors();
        selection_processor->set_type(dspb::SELECTION_TYPE);
        auto selection = selection_processor->mutable_selection();

        dspb::Expr expr_greater_time_1;
        expr_greater_time_1.set_expr_type(dspb::GreaterInt);

        auto expr_int_value = expr_greater_time_1.add_child();
        expr_int_value->set_expr_type(dspb::Column);
        expr_int_value->mutable_column()->CopyFrom(ptr_table_account->GetColumnInfo("id"));

        auto expr_cast_time_to_int = expr_greater_time_1.add_child();
        expr_cast_time_to_int->set_expr_type(dspb::CastTimeToInt);

        auto expr_time_value = expr_cast_time_to_int->add_child();
        expr_time_value->set_expr_type(dspb::Const_Time);

        datatype::MyTime t;
        datatype::MyTime::StWarn st;
        bool ret_b = t.FromString( "10:10:10", st);
        if (!ret_b) {
            t.SetZero();
        }

        expr_time_value->set_value(t.ToString());
        selection->add_filter()->CopyFrom(expr_greater_time_1);

        select_flow->add_output_offsets(0);
        select_flow->add_output_offsets(1);
        select_flow->add_output_offsets(2);

        select_flow->set_gather_trace(false);

        dspb::RangeResponse resp;
        auto s = testSelectFlow(req, resp);
        ASSERT_TRUE(s.ok()) << s.ToString();
        ASSERT_FALSE(resp.header().has_error());
        ASSERT_TRUE(resp.has_select_flow());
        ASSERT_TRUE(resp.select_flow().rows_size() == 9) << resp.select_flow().rows_size();

        {
            for (auto & row : resp.select_flow().rows()) {
                ASSERT_TRUE(mp_kv.find(row.key()) != mp_kv.end())
                        << "Cann't found key:" <<  row.key() << "\tmp_kv.size:" << mp_kv.size();
                ASSERT_TRUE(check_row_value(row.value())) << "check_row_value error:" << row.DebugString();
            }
        }
    }

    //* CastStringToTime(name) > Time(10:10:10)
    {
        dspb::RangeRequest req;
        auto header = req.mutable_header();
        header->set_range_id(default_range_id);
        header->mutable_range_epoch()->set_conf_ver(1);
        header->mutable_range_epoch()->set_version(1);

        auto select_flow = req.mutable_select_flow();
        // add table read processor
        auto table_read_processor = select_flow->add_processors();
        table_read_processor->set_type(dspb::TABLE_READ_TYPE );
        auto table_read = table_read_processor-> mutable_table_read();

        for ( const auto & col : ptr_table_account->GetAllColumns()) {
            table_read->add_columns()->CopyFrom(ptr_table_account->GetColumnInfo(col.id()));
        }

        table_read->set_type(dspb::KEYS_RANGE_TYPE);
        table_read->mutable_range()->set_start_key(start_key);
        table_read->mutable_range()->set_end_key(end_key);
        table_read->set_desc(false);

        auto selection_processor = select_flow->add_processors();
        selection_processor->set_type(dspb::SELECTION_TYPE);
        auto selection = selection_processor->mutable_selection();

        dspb::Expr expr_greater_time_1;
        expr_greater_time_1.set_expr_type(dspb::GreaterTime);

        auto expr_cast_string_to_time = expr_greater_time_1.add_child();
        expr_cast_string_to_time->set_expr_type(dspb::CastStringToTime);

        auto expr_time_column = expr_cast_string_to_time->add_child();
        expr_time_column->set_expr_type(dspb::Column);
        expr_time_column->mutable_column()->CopyFrom(ptr_table_account->GetColumnInfo("name"));

        auto expr_time_value = expr_greater_time_1.add_child();
        expr_time_value->set_expr_type(dspb::Const_Time);

        datatype::MyTime t;
        datatype::MyTime::StWarn st;
        bool ret_b = t.FromString( "10:10:10", st);
        if (!ret_b) {
            t.SetZero();
        }

        expr_time_value->set_value(t.ToString());
        selection->add_filter()->CopyFrom(expr_greater_time_1);

        select_flow->add_output_offsets(0);
        select_flow->add_output_offsets(1);
        select_flow->add_output_offsets(2);

        select_flow->set_gather_trace(false);

        dspb::RangeResponse resp;
        auto s = testSelectFlow(req, resp);
        ASSERT_TRUE(s.ok()) << s.ToString();
        ASSERT_FALSE(resp.header().has_error());
        ASSERT_TRUE(resp.has_select_flow());
        ASSERT_TRUE(resp.select_flow().rows_size() == 9) << resp.select_flow().rows_size();

        {
            for (auto & row : resp.select_flow().rows()) {
                ASSERT_TRUE(mp_kv.find(row.key()) != mp_kv.end())
                        << "Cann't found key:" <<  row.key() << "\tmp_kv.size:" << mp_kv.size();
                ASSERT_TRUE(check_row_value(row.value())) << "check_row_value error:" << row.DebugString();
            }
        }
    }

    //* name > CastTimeToString(Time(10:10:10))
    {
        dspb::RangeRequest req;
        auto header = req.mutable_header();
        header->set_range_id(default_range_id);
        header->mutable_range_epoch()->set_conf_ver(1);
        header->mutable_range_epoch()->set_version(1);

        auto select_flow = req.mutable_select_flow();
        // add table read processor
        auto table_read_processor = select_flow->add_processors();
        table_read_processor->set_type(dspb::TABLE_READ_TYPE );
        auto table_read = table_read_processor-> mutable_table_read();

        for ( const auto & col : ptr_table_account->GetAllColumns()) {
            table_read->add_columns()->CopyFrom(ptr_table_account->GetColumnInfo(col.id()));
        }

        table_read->set_type(dspb::KEYS_RANGE_TYPE);
        table_read->mutable_range()->set_start_key(start_key);
        table_read->mutable_range()->set_end_key(end_key);
        table_read->set_desc(false);

        auto selection_processor = select_flow->add_processors();
        selection_processor->set_type(dspb::SELECTION_TYPE);
        auto selection = selection_processor->mutable_selection();

        dspb::Expr expr_greater_time_1;
        expr_greater_time_1.set_expr_type(dspb::GreaterString);

        auto expr_string_value = expr_greater_time_1.add_child();
        expr_string_value->set_expr_type(dspb::Column);
        expr_string_value->mutable_column()->CopyFrom(ptr_table_account->GetColumnInfo("name"));

        auto expr_cast_time_to_int = expr_greater_time_1.add_child();
        expr_cast_time_to_int->set_expr_type(dspb::CastTimeToString);

        auto expr_time_value = expr_cast_time_to_int->add_child();
        expr_time_value->set_expr_type(dspb::Const_Time);

        datatype::MyTime t;
        datatype::MyTime::StWarn st;
        bool ret_b = t.FromString( "10:10:10", st);
        if (!ret_b) {
            t.SetZero();
        }

        expr_time_value->set_value(t.ToString());
        selection->add_filter()->CopyFrom(expr_greater_time_1);

        select_flow->add_output_offsets(0);
        select_flow->add_output_offsets(1);
        select_flow->add_output_offsets(2);

        select_flow->set_gather_trace(false);

        dspb::RangeResponse resp;
        auto s = testSelectFlow(req, resp);
        ASSERT_TRUE(s.ok()) << s.ToString();
        ASSERT_FALSE(resp.header().has_error());
        ASSERT_TRUE(resp.has_select_flow());
        ASSERT_TRUE(resp.select_flow().rows_size() == 9) << resp.select_flow().rows_size();

        {
            for (auto & row : resp.select_flow().rows()) {
                ASSERT_TRUE(mp_kv.find(row.key()) != mp_kv.end())
                                            << "Cann't found key:" <<  row.key() << "\tmp_kv.size:" << mp_kv.size();
                ASSERT_TRUE(check_row_value(row.value())) << "check_row_value error:" << row.DebugString();
            }
        }
    }


    //* CastIntToTime(id) > CastRealToTime(101010.1010)
    {
        dspb::RangeRequest req;
        auto header = req.mutable_header();
        header->set_range_id(default_range_id);
        header->mutable_range_epoch()->set_conf_ver(1);
        header->mutable_range_epoch()->set_version(1);

        auto select_flow = req.mutable_select_flow();
        // add table read processor
        auto table_read_processor = select_flow->add_processors();
        table_read_processor->set_type(dspb::TABLE_READ_TYPE );
        auto table_read = table_read_processor-> mutable_table_read();

        for ( const auto & col : ptr_table_account->GetAllColumns()) {
            table_read->add_columns()->CopyFrom(ptr_table_account->GetColumnInfo(col.id()));
        }

        table_read->set_type(dspb::KEYS_RANGE_TYPE);
        table_read->mutable_range()->set_start_key(start_key);
        table_read->mutable_range()->set_end_key(end_key);
        table_read->set_desc(false);

        auto selection_processor = select_flow->add_processors();
        selection_processor->set_type(dspb::SELECTION_TYPE);
        auto selection = selection_processor->mutable_selection();

        dspb::Expr expr_greater_time_1;
        expr_greater_time_1.set_expr_type(dspb::GreaterTime);

        auto expr_cast_int_to_time = expr_greater_time_1.add_child();
        expr_cast_int_to_time->set_expr_type(dspb::CastIntToTime);

        auto expr_time_column = expr_cast_int_to_time->add_child();
        expr_time_column->set_expr_type(dspb::Column);
        expr_time_column->mutable_column()->CopyFrom(ptr_table_account->GetColumnInfo("id"));

        auto expr_cast_real_to_time_value = expr_greater_time_1.add_child();
        expr_cast_real_to_time_value->set_expr_type(dspb::CastRealToTime);

        auto expr_real_value = expr_cast_real_to_time_value->add_child();
        expr_real_value->set_expr_type(dspb::Const_Double);

        double d = 101010.1010;
        std::string str_d = std::to_string(d);

        expr_real_value->set_value(str_d);
        selection->add_filter()->CopyFrom(expr_greater_time_1);

        select_flow->add_output_offsets(0);
        select_flow->add_output_offsets(1);
        select_flow->add_output_offsets(2);

        select_flow->set_gather_trace(false);

        dspb::RangeResponse resp;
        auto s = testSelectFlow(req, resp);
        ASSERT_TRUE(s.ok()) << s.ToString();
        ASSERT_FALSE(resp.header().has_error());
        ASSERT_TRUE(resp.has_select_flow());
        ASSERT_TRUE(resp.select_flow().rows_size() == 9) << resp.select_flow().rows_size();

        {
            for (auto & row : resp.select_flow().rows()) {
                ASSERT_TRUE(mp_kv.find(row.key()) != mp_kv.end())
                                            << "Cann't found key:" <<  row.key() << "\tmp_kv.size:" << mp_kv.size();
                ASSERT_TRUE(check_row_value(row.value())) << "check_row_value error:" << row.DebugString();
            }
        }
    }
    //* CastIntToReal(id) > CastTimeToReal(Time(10:10:10))
    {
        dspb::RangeRequest req;
        auto header = req.mutable_header();
        header->set_range_id(default_range_id);
        header->mutable_range_epoch()->set_conf_ver(1);
        header->mutable_range_epoch()->set_version(1);

        auto select_flow = req.mutable_select_flow();
        // add table read processor
        auto table_read_processor = select_flow->add_processors();
        table_read_processor->set_type(dspb::TABLE_READ_TYPE );
        auto table_read = table_read_processor-> mutable_table_read();

        for ( const auto & col : ptr_table_account->GetAllColumns()) {
            table_read->add_columns()->CopyFrom(ptr_table_account->GetColumnInfo(col.id()));
        }

        table_read->set_type(dspb::KEYS_RANGE_TYPE);
        table_read->mutable_range()->set_start_key(start_key);
        table_read->mutable_range()->set_end_key(end_key);
        table_read->set_desc(false);

        auto selection_processor = select_flow->add_processors();
        selection_processor->set_type(dspb::SELECTION_TYPE);
        auto selection = selection_processor->mutable_selection();

        dspb::Expr expr_greater_time_1;
        expr_greater_time_1.set_expr_type(dspb::GreaterReal);

        auto expr_cast_int_to_real = expr_greater_time_1.add_child();
        expr_cast_int_to_real->set_expr_type(dspb::CastIntToReal);

        auto expr_time_column = expr_cast_int_to_real->add_child();
        expr_time_column->set_expr_type(dspb::Column);
        expr_time_column->mutable_column()->CopyFrom(ptr_table_account->GetColumnInfo("id"));

        auto expr_cast_time_to_real_value = expr_greater_time_1.add_child();
        expr_cast_time_to_real_value->set_expr_type(dspb::CastTimeToReal);

        auto expr_time_value = expr_cast_time_to_real_value->add_child();
        expr_time_value->set_expr_type(dspb::Const_Time);

        datatype::MyTime t;
        datatype::MyTime::StWarn st;
        bool ret_b = t.FromString( "10:10:10", st);
        if (!ret_b) {
            t.SetZero();
        }

        expr_time_value->set_value(t.ToString());
        selection->add_filter()->CopyFrom(expr_greater_time_1);

        select_flow->add_output_offsets(0);
        select_flow->add_output_offsets(1);
        select_flow->add_output_offsets(2);

        select_flow->set_gather_trace(false);

        dspb::RangeResponse resp;
        auto s = testSelectFlow(req, resp);
        ASSERT_TRUE(s.ok()) << s.ToString();
        ASSERT_FALSE(resp.header().has_error());
        ASSERT_TRUE(resp.has_select_flow());
        ASSERT_TRUE(resp.select_flow().rows_size() == 9) << resp.select_flow().rows_size();

        {
            for (auto & row : resp.select_flow().rows()) {
                ASSERT_TRUE(mp_kv.find(row.key()) != mp_kv.end())
                                            << "Cann't found key:" <<  row.key() << "\tmp_kv.size:" << mp_kv.size();
                ASSERT_TRUE(check_row_value(row.value())) << "check_row_value error:" << row.DebugString();
            }
        }
    }

    //* CastIntToTime(id) > CastDecimalToTime(Decimal(101010.101010))
    {
        dspb::RangeRequest req;
        auto header = req.mutable_header();
        header->set_range_id(default_range_id);
        header->mutable_range_epoch()->set_conf_ver(1);
        header->mutable_range_epoch()->set_version(1);

        auto select_flow = req.mutable_select_flow();
        // add table read processor
        auto table_read_processor = select_flow->add_processors();
        table_read_processor->set_type(dspb::TABLE_READ_TYPE );
        auto table_read = table_read_processor-> mutable_table_read();

        for ( const auto & col : ptr_table_account->GetAllColumns()) {
            table_read->add_columns()->CopyFrom(ptr_table_account->GetColumnInfo(col.id()));
        }

        table_read->set_type(dspb::KEYS_RANGE_TYPE);
        table_read->mutable_range()->set_start_key(start_key);
        table_read->mutable_range()->set_end_key(end_key);
        table_read->set_desc(false);

        auto selection_processor = select_flow->add_processors();
        selection_processor->set_type(dspb::SELECTION_TYPE);
        auto selection = selection_processor->mutable_selection();

        dspb::Expr expr_greater_time_1;
        expr_greater_time_1.set_expr_type(dspb::GreaterTime);

        auto expr_cast_int_to_time = expr_greater_time_1.add_child();
        expr_cast_int_to_time->set_expr_type(dspb::CastIntToTime);

        auto expr_time_column = expr_cast_int_to_time->add_child();
        expr_time_column->set_expr_type(dspb::Column);
        expr_time_column->mutable_column()->CopyFrom(ptr_table_account->GetColumnInfo("id"));

        auto expr_cast_decimal_to_time_value = expr_greater_time_1.add_child();
        expr_cast_decimal_to_time_value->set_expr_type(dspb::CastDecimalToTime);

        auto expr_decimal_value = expr_cast_decimal_to_time_value->add_child();
        expr_decimal_value->set_expr_type(dspb::Const_Decimal);

        datatype::MyDecimal dec;
        auto ret_data = dec.FromString("101010.101010");
        if (ret_data != 0) {
            dec.ResetZero();
        }

        expr_decimal_value->set_value(dec.ToString());

        selection->add_filter()->CopyFrom(expr_greater_time_1);

        select_flow->add_output_offsets(0);
        select_flow->add_output_offsets(1);
        select_flow->add_output_offsets(2);

        select_flow->set_gather_trace(false);

        dspb::RangeResponse resp;
        auto s = testSelectFlow(req, resp);
        ASSERT_TRUE(s.ok()) << s.ToString();
        ASSERT_FALSE(resp.header().has_error());
        ASSERT_TRUE(resp.has_select_flow());
        ASSERT_TRUE(resp.select_flow().rows_size() == 9) << resp.select_flow().rows_size();

        {
            for (auto & row : resp.select_flow().rows()) {
                ASSERT_TRUE(mp_kv.find(row.key()) != mp_kv.end())
                                            << "Cann't found key:" <<  row.key() << "\tmp_kv.size:" << mp_kv.size();
                ASSERT_TRUE(check_row_value(row.value())) << "check_row_value error:" << row.DebugString();
            }
        }
    }

    //* CastIntToDecimal(id) > CastTimeToDecimal(Time(10:10:10))
    {
        dspb::RangeRequest req;
        auto header = req.mutable_header();
        header->set_range_id(default_range_id);
        header->mutable_range_epoch()->set_conf_ver(1);
        header->mutable_range_epoch()->set_version(1);

        auto select_flow = req.mutable_select_flow();
        // add table read processor
        auto table_read_processor = select_flow->add_processors();
        table_read_processor->set_type(dspb::TABLE_READ_TYPE );
        auto table_read = table_read_processor-> mutable_table_read();

        for ( const auto & col : ptr_table_account->GetAllColumns()) {
            table_read->add_columns()->CopyFrom(ptr_table_account->GetColumnInfo(col.id()));
        }

        table_read->set_type(dspb::KEYS_RANGE_TYPE);
        table_read->mutable_range()->set_start_key(start_key);
        table_read->mutable_range()->set_end_key(end_key);
        table_read->set_desc(false);

        auto selection_processor = select_flow->add_processors();
        selection_processor->set_type(dspb::SELECTION_TYPE);

        auto selection = selection_processor->mutable_selection();

        dspb::Expr expr_greater_time_1;
        expr_greater_time_1.set_expr_type(dspb::GreaterDecimal);

        auto expr_cast_int_to_decimal = expr_greater_time_1.add_child();
        expr_cast_int_to_decimal->set_expr_type(dspb::CastIntToDecimal);

        auto expr_decimal_column = expr_cast_int_to_decimal->add_child();
        expr_decimal_column->set_expr_type(dspb::Column);
        expr_decimal_column->mutable_column()->CopyFrom(ptr_table_account->GetColumnInfo("id"));

        auto expr_cast_time_to_decimal_value = expr_greater_time_1.add_child();
        expr_cast_time_to_decimal_value->set_expr_type(dspb::CastTimeToDecimal);

        auto expr_time_value = expr_cast_time_to_decimal_value->add_child();
        expr_time_value->set_expr_type(dspb::Const_Time);

        datatype::MyTime t;
        datatype::MyTime::StWarn st;
        bool ret_b = t.FromString( "10:10:10", st);
        if (!ret_b) {
            t.SetZero();
        }

        expr_time_value->set_value(t.ToString());

        selection->add_filter()->CopyFrom(expr_greater_time_1);

        select_flow->add_output_offsets(0);
        select_flow->add_output_offsets(1);
        select_flow->add_output_offsets(2);

        select_flow->set_gather_trace(false);

        dspb::RangeResponse resp;
        auto s = testSelectFlow(req, resp);
        ASSERT_TRUE(s.ok()) << s.ToString();
        ASSERT_FALSE(resp.header().has_error());
        ASSERT_TRUE(resp.has_select_flow());
        ASSERT_TRUE(resp.select_flow().rows_size() == 9) << resp.select_flow().rows_size();

        {
            for (auto & row : resp.select_flow().rows()) {
                ASSERT_TRUE(mp_kv.find(row.key()) != mp_kv.end())
                                            << "Cann't found key:" <<  row.key() << "\tmp_kv.size:" << mp_kv.size();
                ASSERT_TRUE(check_row_value(row.value())) << "check_row_value error:" << row.DebugString();
            }
        }
    }


    //* CastIntToTime(id) > CastDateToTime(DateTime(2019-11-20 10:10:10))
    {
        dspb::RangeRequest req;
        auto header = req.mutable_header();
        header->set_range_id(default_range_id);
        header->mutable_range_epoch()->set_conf_ver(1);
        header->mutable_range_epoch()->set_version(1);

        auto select_flow = req.mutable_select_flow();
        // add table read processor
        auto table_read_processor = select_flow->add_processors();
        table_read_processor->set_type(dspb::TABLE_READ_TYPE );
        auto table_read = table_read_processor-> mutable_table_read();

        for ( const auto & col : ptr_table_account->GetAllColumns()) {
            table_read->add_columns()->CopyFrom(ptr_table_account->GetColumnInfo(col.id()));
        }

        table_read->set_type(dspb::KEYS_RANGE_TYPE);
        table_read->mutable_range()->set_start_key(start_key);
        table_read->mutable_range()->set_end_key(end_key);
        table_read->set_desc(false);

        auto selection_processor = select_flow->add_processors();
        selection_processor->set_type(dspb::SELECTION_TYPE);
        auto selection = selection_processor->mutable_selection();

        dspb::Expr expr_greater_time_1;
        expr_greater_time_1.set_expr_type(dspb::GreaterTime);

        auto expr_cast_int_to_time = expr_greater_time_1.add_child();
        expr_cast_int_to_time->set_expr_type(dspb::CastIntToTime);

        auto expr_time_column = expr_cast_int_to_time->add_child();
        expr_time_column->set_expr_type(dspb::Column);
        expr_time_column->mutable_column()->CopyFrom(ptr_table_account->GetColumnInfo("id"));

        auto expr_cast_date_to_time_value = expr_greater_time_1.add_child();
        expr_cast_date_to_time_value->set_expr_type(dspb::CastDateToTime);

        auto expr_date_value = expr_cast_date_to_time_value->add_child();
        expr_date_value->set_expr_type(dspb::Const_Date);

        datatype::MyDateTime dt;
        datatype::MyDateTime::StWarn st;
        auto ret_b = dt.FromString("2000-10-10 10:10:10", st);
        if (!ret_b) {
            dt.SetZero();
        }
        expr_date_value->set_value(dt.ToString());

        selection->add_filter()->CopyFrom(expr_greater_time_1);

        select_flow->add_output_offsets(0);
        select_flow->add_output_offsets(1);
        select_flow->add_output_offsets(2);

        select_flow->set_gather_trace(false);

        dspb::RangeResponse resp;
        auto s = testSelectFlow(req, resp);
        ASSERT_TRUE(s.ok()) << s.ToString();
        ASSERT_FALSE(resp.header().has_error());
        ASSERT_TRUE(resp.has_select_flow());
        ASSERT_TRUE(resp.select_flow().rows_size() == 9) << resp.select_flow().rows_size();

        {
            for (auto & row : resp.select_flow().rows()) {
                ASSERT_TRUE(mp_kv.find(row.key()) != mp_kv.end())
                                            << "Cann't found key:" <<  row.key() << "\tmp_kv.size:" << mp_kv.size();
                ASSERT_TRUE(check_row_value(row.value())) << "check_row_value error:" << row.DebugString();
            }
        }
    }

    //* CastTimeToDate(CastIntToTime(id)) > CastTimeToDate(Time(10:10:10))
    {
        dspb::RangeRequest req;
        auto header = req.mutable_header();
        header->set_range_id(default_range_id);
        header->mutable_range_epoch()->set_conf_ver(1);
        header->mutable_range_epoch()->set_version(1);

        auto select_flow = req.mutable_select_flow();
        // add table read processor
        auto table_read_processor = select_flow->add_processors();
        table_read_processor->set_type(dspb::TABLE_READ_TYPE );
        auto table_read = table_read_processor-> mutable_table_read();

        for ( const auto & col : ptr_table_account->GetAllColumns()) {
            table_read->add_columns()->CopyFrom(ptr_table_account->GetColumnInfo(col.id()));
        }

        table_read->set_type(dspb::KEYS_RANGE_TYPE);
        table_read->mutable_range()->set_start_key(start_key);
        table_read->mutable_range()->set_end_key(end_key);
        table_read->set_desc(false);

        auto selection_processor = select_flow->add_processors();
        selection_processor->set_type(dspb::SELECTION_TYPE);
        auto selection = selection_processor->mutable_selection();

        dspb::Expr expr_greater_time_1;
        expr_greater_time_1.set_expr_type(dspb::GreaterDate);

        auto expr_cast_time_to_date = expr_greater_time_1.add_child();
        expr_cast_time_to_date->set_expr_type(dspb::CastTimeToDate);

        auto expr_cast_int_to_time = expr_cast_time_to_date->add_child();
        expr_cast_int_to_time->set_expr_type(dspb::CastIntToTime);

        auto expr_time_column = expr_cast_int_to_time->add_child();
        expr_time_column->set_expr_type(dspb::Column);
        expr_time_column->mutable_column()->CopyFrom(ptr_table_account->GetColumnInfo("id"));

        auto expr_cast_time_to_date_value = expr_greater_time_1.add_child();
        expr_cast_time_to_date_value->set_expr_type(dspb::CastTimeToDate);

        auto expr_time_value = expr_cast_time_to_date_value->add_child();
        expr_time_value->set_expr_type(dspb::Const_Time);

        datatype::MyTime t;
        datatype::MyTime::StWarn st;
        bool ret_b = t.FromString( "10:10:10", st);
        if (!ret_b) {
            t.SetZero();
        }

        expr_time_value->set_value(t.ToString());

        selection->add_filter()->CopyFrom(expr_greater_time_1);

        select_flow->add_output_offsets(0);
        select_flow->add_output_offsets(1);
        select_flow->add_output_offsets(2);

        select_flow->set_gather_trace(false);

        dspb::RangeResponse resp;
        auto s = testSelectFlow(req, resp);
        ASSERT_TRUE(s.ok()) << s.ToString();
        ASSERT_FALSE(resp.header().has_error());
        ASSERT_TRUE(resp.has_select_flow());
        ASSERT_TRUE(resp.select_flow().rows_size() == 9) << resp.select_flow().rows_size();

        {
            for (auto & row : resp.select_flow().rows()) {
                ASSERT_TRUE(mp_kv.find(row.key()) != mp_kv.end())
                                            << "Cann't found key:" <<  row.key() << "\tmp_kv.size:" << mp_kv.size();
                ASSERT_TRUE(check_row_value(row.value())) << "check_row_value error:" << row.DebugString();
            }
        }
    }


    //* CastIntToTime(id) > CastTimeToTime(Time(10:10:10))

}

