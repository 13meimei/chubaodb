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

#include "helper/cpp_permission.h"
#include "range/range.h"
#include "server/range_tree.h"
#include "server/range_server.h"

#include "helper/table.h"
#include "helper/mock/raft_mock.h"
#include "helper/mock/rpc_request_mock.h"
#include "helper/helper_util.h"

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
 * =========================================
 * |  id  |       name        |   balance  |
 * =========================================
 * | 1000 |    name_1000      |     1000   |
 * | 1001 |    name_1001      |     1001   |
 * | 1002 |    name_1002      |     1002   |
 * | 1003 |    name_1003      |     1003   |
 * | 1004 |    name_1004      |     1004   |
 * | 1005 |    name_1005      |     1005   |
 * | 1006 |    name_1006      |     1006   |
 * | 1007 |    name_1007      |     1007   |
 * | 1008 |    name_1008      |     1008   |
 * | 1009 |    name_1009      |     1009   |
 * | 1010 |    name_1010      |     1010   |
 * | 1011 |    name_1011      |     1011   |
 * | 1012 |    name_1012      |     1012   |
 * | 1013 |    name_1013      |     1013   |
 * | 1014 |    name_1014      |     1014   |
 * | 1015 |    name_1015      |     1015   |
 * | 1016 |    name_1016      |     1016   |
 * | 1017 |    name_1017      |     1017   |
 * | 1018 |    name_1018      |     1018   |
 * | 1019 |    name_1019      |     1019   |
 * | 1020 |    name_1020      |     1020   |
 * =========================================
 *
 *
 * */

basepb::Range *genRange() {
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

TEST_F(SelectFlowTest, SelectFlowAndAgg) {
    std::string start_key;
    std::string end_key;
    std::string end_key_1;
    long long data_count = 20;

    { // create table
        EncodeKeyPrefix(&start_key, default_account_table_id);
        EncodeKeyPrefix(&end_key, default_account_table_id+1);
        EncodeKeyPrefix(&end_key_1, default_account_table_id+2);
    }


    { // begin create range
        dspb::SchReuqest req;
        req.mutable_create_range()->set_allocated_range(genRange());

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

        typedef int col_id_type;

        for (auto id = 0; id < data_count; id++) {
            std::string key;
            std::string value;

            // make row data
            std::map<col_id_type, std::string> row;
            row.emplace(1, std::to_string(1000+id));
            row.emplace(2, "name_" + std::to_string(1000+id));
            row.emplace(3, std::to_string(1000+id));

            // make encode key
            EncodeKeyPrefix(&key, default_account_table_id); // key:100000001 ("\x01"+"00000001")
            for ( auto & col : ptr_table_account->GetPKs()) {
                EncodePrimaryKey(&key, col,row.at(col.id()));   // key: key+ row_id(encoded)
            }

            // make encode value
            for (auto & col : ptr_table_account->GetNonPkColumns()) {
                EncodeColumnValue(&value, col, row.at(col.id()));
            }

            // put data to db
            dspb::RangeRequest req;
            req.mutable_header()->set_range_id(default_range_id);
            req.mutable_header()->mutable_range_epoch()->set_conf_ver(1);
            req.mutable_header()->mutable_range_epoch()->set_version(1);
            req.mutable_kv_put()->set_key(key);
            req.mutable_kv_put()->set_value(value);

            dspb::RangeResponse resp;
            auto s = testKv(req, resp);
            ASSERT_TRUE(s.ok()) << s.ToString();
            ASSERT_FALSE(resp.header().has_error());
        }
    }

    { // test select and order by and limit
        // table_read;

        dspb::RangeRequest req;
        auto header = req.mutable_header();
        header->set_range_id(default_range_id);
        header->mutable_range_epoch()->set_conf_ver(1);
        header->mutable_range_epoch()->set_version(1);

        auto select_flow = req.mutable_select_flow();

        // table_read
        auto table_read_processor = select_flow->add_processors();
        table_read_processor->set_type(dspb::TABLE_READ_TYPE );
        auto table_read = table_read_processor->mutable_table_read();
        for ( const auto & col : ptr_table_account->GetAllColumns()) {
            // table_read->add_columns()->CopyFrom(ptr_table_account->GetColumnInfo(col.id()));
        }

        table_read->set_type(dspb::KEYS_RANGE_TYPE);
        table_read->mutable_range()->set_start_key(start_key);
        table_read->mutable_range()->set_end_key(end_key);
        table_read->set_desc(false);

        // agg
        auto agg_processor = select_flow->add_processors();
        agg_processor->set_type(dspb::AGGREGATION_TYPE);
        auto agg = agg_processor->mutable_aggregation();

        dspb::Expr *func_tmp = nullptr;
        dspb::Expr *child = nullptr;
        //avg
        // func_tmp = agg->add_func();
        // func_tmp->set_expr_type(dspb::Avg);
        // std::string cl_name = "id";
        // auto cl_table = ptr_table_account->GetColumn(cl_name);
        // func_tmp->mutable_column()->set_id(cl_table.id());

        //count(id)
        // func_tmp = agg->add_func();
        // func_tmp->set_expr_type(dspb::Count);
        // cl_name = "id";
        // cl_table = ptr_table_account->GetColumn(cl_name);
        // auto child = func_tmp->add_child();
        // child->set_expr_type(dspb::Column);
        // child->mutable_column()->set_id(cl_table.id());

        //count(1)
         func_tmp = agg->add_func();
        func_tmp->set_expr_type(dspb::Count);
        // child = func_tmp->add_child();
        // child->set_expr_type(dspb::Const_Int);
        // child->set_value("1");
        
        //Max
        // func_tmp = agg->add_func();
        // func_tmp->set_expr_type(dspb::Max);
        // cl_name = "id";
        // cl_table = ptr_table_account->GetColumn(cl_name);
        // func_tmp->mutable_column()->set_id(cl_table.id());

        // //Min
        // func_tmp = agg->add_func();
        // func_tmp->set_expr_type(dspb::Min);
        // cl_name = "id";
        // cl_table = ptr_table_account->GetColumn(cl_name);
        // func_tmp->mutable_column()->set_id(cl_table.id());

        // //Sum
        // func_tmp = agg->add_func();
        // func_tmp->set_expr_type(dspb::Sum);
        // cl_name = "id";
        // cl_table = ptr_table_account->GetColumn(cl_name);
        // func_tmp->mutable_column()->set_id(cl_table.id());

        select_flow->set_gather_trace(false);

        dspb::RangeResponse resp;
        auto s = testSelectFlow(req, resp);
        ASSERT_TRUE(s.ok()) << s.ToString();
        ASSERT_FALSE(resp.header().has_error());
        auto selectFlowResp = resp.select_flow();
        // std::cout << "row size:" << selectFlowResp.rows().size() << std::endl;
        std::cout << "selectFlowResp:\n" << selectFlowResp.DebugString() << std::endl;
        int nSum = 0;
        for (int i = 0; i < data_count; ++i) {
            nSum += 1000 + i;
        }
        // std::cout << "sum:" << nSum << " agv:" << nSum/data_count << std::endl;
        size_t offset = 0;
        int64_t value = 0;
        //avg
        std::string fields = selectFlowResp.rows()[0].value().fields();
        // ASSERT_TRUE(DecodeNonSortingVarint(fields, ++offset, &value));
        // std::cout << "avg sum:" << value << std::endl;
        // ASSERT_EQ(value, nSum);
        // ASSERT_TRUE(DecodeNonSortingVarint(fields, ++offset, &value));
        // std::cout << "avg count:" << value << std::endl;
        // ASSERT_EQ(value, data_count);

        // //count(id)
        // ASSERT_TRUE(DecodeNonSortingVarint(fields, ++offset, &value));
        // std::cout << "count(id):" << value << std::endl;
        // ASSERT_EQ(value, data_count);
        //count(1)
        ASSERT_TRUE(DecodeNonSortingVarint(fields, ++offset, &value));
        std::cout << "count(1):" << value << std::endl;
        ASSERT_EQ(value, data_count);
        // //max
        // ASSERT_TRUE(DecodeNonSortingVarint(fields, ++offset, &value));
        // std::cout << "max:" << value << std::endl;
        // ASSERT_EQ(value, 1019);
        // //min
        // ASSERT_TRUE(DecodeNonSortingVarint(fields, ++offset, &value));
        // std::cout << "min:" << value << std::endl;
        // ASSERT_EQ(value, 1000);
        // //sum
        // ASSERT_TRUE(DecodeNonSortingVarint(fields, ++offset, &value));
        // std::cout << "sum:" << value << std::endl;
        // ASSERT_EQ(value, nSum);
    }
}

TEST_F(SelectFlowTest, SelectFlowAndOrdering) {
    std::string start_key;
    std::string end_key;
    std::string end_key_1;

    { // create table
        EncodeKeyPrefix(&start_key, default_account_table_id);
        EncodeKeyPrefix(&end_key, default_account_table_id+1);
        EncodeKeyPrefix(&end_key_1, default_account_table_id+2);
    }


    { // begin create range
        dspb::SchReuqest req;
        req.mutable_create_range()->set_allocated_range(genRange());

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

        typedef int col_id_type;

        for (auto id = 0; id < 20; id++) {
            std::string key;
            std::string value;

            // make row data
            std::map<col_id_type, std::string> row;
            row.emplace(1, std::to_string(1000+id));
            row.emplace(2, "name_" + std::to_string(1000+id));
            row.emplace(3, std::to_string(1000+id));

            // make encode key
            EncodeKeyPrefix(&key, default_account_table_id);
            for ( auto & col : ptr_table_account->GetPKs()) {
                EncodePrimaryKey(&key, col,row.at(col.id()));
            }

            // make encode value
            for (auto & col : ptr_table_account->GetNonPkColumns()) {
                EncodeColumnValue(&value, col, row.at(col.id()));
            }

            // put data to db
            dspb::RangeRequest req;
            req.mutable_header()->set_range_id(default_range_id);
            req.mutable_header()->mutable_range_epoch()->set_conf_ver(1);
            req.mutable_header()->mutable_range_epoch()->set_version(1);
            req.mutable_kv_put()->set_key(key);
            req.mutable_kv_put()->set_value(value);

            dspb::RangeResponse resp;
            auto s = testKv(req, resp);
            ASSERT_TRUE(s.ok()) << s.ToString();
            ASSERT_FALSE(resp.header().has_error());
        }
    }

    { // test select and order by
        // table_read;

        dspb::RangeRequest req;
        auto header = req.mutable_header();
        header->set_range_id(default_range_id);
        header->mutable_range_epoch()->set_conf_ver(1);
        header->mutable_range_epoch()->set_version(1);

        auto select_flow = req.mutable_select_flow();

        // table_read
        auto table_read_processor = select_flow->add_processors();
        table_read_processor->set_type(dspb::TABLE_READ_TYPE );
        auto table_read = table_read_processor->mutable_table_read();
        for ( const auto & col : ptr_table_account->GetAllColumns()) {
            table_read->add_columns()->CopyFrom(ptr_table_account->GetColumnInfo(col.id()));
        }

        table_read->set_type(dspb::KEYS_RANGE_TYPE);
        table_read->mutable_range()->set_start_key(start_key);
        table_read->mutable_range()->set_end_key(end_key);
        table_read->set_desc(false);

        // order by
        auto order_by_processor = select_flow->add_processors();
        order_by_processor->set_type(dspb::ORDER_BY_TYPE);
        auto ordering = order_by_processor->mutable_ordering();
        std::string cl_name = "id";
        auto cl_table = ptr_table_account->GetColumn(cl_name);
        // auto cl1 = ordering->add_columns();
        // cl1->set_asc(false);
        // cl1->mutable_expr()->set_expr_type(dspb::Column);
        // cl1->mutable_expr()->mutable_column()->set_id(cl_table.id());
        // cl_name = "name";
        // cl_table = ptr_table_account->GetColumn(cl_name);
        // auto cl2 = ordering->add_columns();
        // cl2->set_asc(false);
        // cl2->mutable_expr()->set_expr_type(dspb::Column);
        // cl2->mutable_expr()->mutable_column()->set_id(cl_table.id());
        cl_name = "balance";
        cl_table = ptr_table_account->GetColumn(cl_name);
        auto cl3 = ordering->add_columns();
        cl3->set_asc(false);
        cl3->mutable_expr()->set_expr_type(dspb::Column);
        cl3->mutable_expr()->mutable_column()->set_id(cl_table.id());
        int64_t count = 2;
        ordering->set_count(count);

        select_flow->add_output_offsets(0);
        select_flow->add_output_offsets(1);
        select_flow->add_output_offsets(2);

        select_flow->set_gather_trace(false);

        dspb::RangeResponse resp;
        auto s = testSelectFlow(req, resp);
        ASSERT_TRUE(s.ok()) << s.ToString();
        ASSERT_FALSE(resp.header().has_error());
        auto selectFlowResp = resp.select_flow();
        ASSERT_EQ(selectFlowResp.rows().size(), count);
        // std::cout << "row size:" << selectFlowResp.rows().size() << std::endl;
        // std::cout << "selectFlowResp:\n" << selectFlowResp.DebugString() << std::endl;
    }
}

TEST_F(SelectFlowTest, SelectFlowLimit) {

    std::string start_key;
    std::string end_key;
    std::string end_key_1;

    { // create table
        EncodeKeyPrefix(&start_key, default_account_table_id);
        EncodeKeyPrefix(&end_key, default_account_table_id+1);
        EncodeKeyPrefix(&end_key_1, default_account_table_id+2);
    }


    { // begin create range
        dspb::SchReuqest req;
        req.mutable_create_range()->set_allocated_range(genRange());

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

        typedef int col_id_type;

        for (auto id = 0; id < 20; id++) {
            std::string key;
            std::string value;

            // make row data
            std::map<col_id_type, std::string> row;
            row.emplace(1, std::to_string(1000+id));
            row.emplace(2, "name_" + std::to_string(1000+id));
            row.emplace(3, std::to_string(1000+id));

            // make encode key
            EncodeKeyPrefix(&key, default_account_table_id);
            for ( auto & col : ptr_table_account->GetPKs()) {
                EncodePrimaryKey(&key, col,row.at(col.id()));
            }

            // make encode value
            for (auto & col : ptr_table_account->GetNonPkColumns()) {
                EncodeColumnValue(&value, col, row.at(col.id()));
            }

            // put data to db
            dspb::RangeRequest req;
            req.mutable_header()->set_range_id(default_range_id);
            req.mutable_header()->mutable_range_epoch()->set_conf_ver(1);
            req.mutable_header()->mutable_range_epoch()->set_version(1);
            req.mutable_kv_put()->set_key(key);
            req.mutable_kv_put()->set_value(value);

            dspb::RangeResponse resp;
            auto s = testKv(req, resp);
            ASSERT_TRUE(s.ok()) << s.ToString();
            ASSERT_FALSE(resp.header().has_error());
        }
    }

    { // test select and limit
        // table_read;

        dspb::RangeRequest req;
        auto header = req.mutable_header();
        header->set_range_id(default_range_id);
        header->mutable_range_epoch()->set_conf_ver(1);
        header->mutable_range_epoch()->set_version(1);

        auto select_flow = req.mutable_select_flow();

        // table_read
        auto table_read_processor = select_flow->add_processors();
        table_read_processor->set_type(dspb::TABLE_READ_TYPE );
        auto table_read = table_read_processor->mutable_table_read();
        for ( const auto & col : ptr_table_account->GetAllColumns()) {
            table_read->add_columns()->CopyFrom(ptr_table_account->GetColumnInfo(col.id()));
        }

        table_read->set_type(dspb::KEYS_RANGE_TYPE);
        table_read->mutable_range()->set_start_key(start_key);
        table_read->mutable_range()->set_end_key(end_key);
        table_read->set_desc(false);

        // limit
        auto limit_processor = select_flow->add_processors();
        limit_processor->set_type(dspb::LIMIT_TYPE);
        auto limit_read = limit_processor->mutable_limit();
        uint64_t count = 3;
        uint64_t offset = 2;
        limit_read->set_count(count);
        limit_read->set_offset(offset);

        select_flow->add_output_offsets(0);
        select_flow->add_output_offsets(1);
        select_flow->add_output_offsets(2);

        select_flow->set_gather_trace(false);

        dspb::RangeResponse resp;
        auto s = testSelectFlow(req, resp);
        ASSERT_TRUE(s.ok()) << s.ToString();
        ASSERT_FALSE(resp.header().has_error());
        auto selectFlowResp = resp.select_flow();
        ASSERT_EQ(selectFlowResp.rows().size(), int64_t(count));
        // std::cout << "row size:" << selectFlowResp.rows().size() << std::endl;
        // std::cout << "selectFlowResp:\n" << selectFlowResp.DebugString() << std::endl;
        
    }
}