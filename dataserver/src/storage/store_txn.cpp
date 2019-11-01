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

#include "store.h"

#include "base/util.h"
#include "common/logger.h"
#include "common/ds_encoding.h"
#include "row_fetcher.h"
#include "processor.h"
#include "processor_table_read.h"
#include "processor_index_read.h"
#include "processor_selection.h"
#include "processor_limit.h"
#include "processor_agg.h"
#include "processor_order_by.h"
#include "processor_data_sample.h"

#include "util.h"

namespace chubaodb {
namespace ds {
namespace storage {

using namespace dspb;

// TODO: add metrics
static void fillTxnValue(const PrepareRequest& req, const TxnIntent& intent, uint64_t version, TxnValue* value) {
    value->set_txn_id(req.txn_id());
    value->mutable_intent()->CopyFrom(intent);
    value->set_primary_key(req.primary_key());
    value->set_expired_at(calExpireAt(req.lock_ttl()));
    value->set_version(version);
    if (intent.is_primary()) {
        for (const auto& key: req.secondary_keys()) {
            value->add_secondary_keys(key);
        }
    }
}

static void setTxnServerErr(TxnError* err, int32_t code, const std::string& msg) {
    err->set_err_type(TxnError_ErrType_SERVER_ERROR);
    err->mutable_server_err()->set_code(code);
    err->mutable_server_err()->set_msg(msg);
}

static TxnErrorPtr newTxnServerErr(int32_t code, const std::string& msg) {
    TxnErrorPtr err(new TxnError);
    setTxnServerErr(err.get(), code, msg);
    return err;
}

static void fillLockInfo(LockInfo* lock_info, const TxnValue& value) {
    lock_info->set_txn_id(value.txn_id());
    lock_info->set_timeout(isExpired(value.expired_at()));
    lock_info->set_is_primary(value.intent().is_primary());
    lock_info->set_primary_key(value.primary_key());
    if (value.intent().is_primary()) {
        lock_info->set_status(value.txn_status());
        for (const auto& skey: value.secondary_keys()) {
            lock_info->add_secondary_keys(skey);
        }
    }
}

static TxnErrorPtr newLockedError(const TxnValue& value) {
    TxnErrorPtr err(new TxnError);
    err->set_err_type(TxnError_ErrType_LOCKED);
    auto lock_err = err->mutable_lock_err();
    lock_err->set_key(value.intent().key());
    fillLockInfo(lock_err->mutable_info(), value);
    return err;
}

static TxnErrorPtr newStatusConflictErr(TxnStatus status) {
    TxnErrorPtr err(new TxnError);
    err->set_err_type(TxnError_ErrType_STATUS_CONFLICT);
    err->mutable_status_conflict()->set_status(status);
    return err;
}

static void setNotFoundErr(TxnError* err, const std::string& key) {
    err->set_err_type(TxnError_ErrType_NOT_FOUND);
    err->mutable_not_found()->set_key(key);
}

static TxnErrorPtr newNotFoundErr(const std::string& key) {
    TxnErrorPtr err(new TxnError);
    setNotFoundErr(err.get(), key);
    return err;
}

static TxnErrorPtr newUnexpectedVerErr(const std::string& key, uint64_t expected, uint64_t actual) {
    TxnErrorPtr err(new TxnError);
    err->set_err_type(TxnError_ErrType_UNEXPECTED_VER);
    err->mutable_unexpected_ver()->set_key(key);
    err->mutable_unexpected_ver()->set_expected_ver(expected);
    err->mutable_unexpected_ver()->set_actual_ver(actual);
    return err;
}

static TxnErrorPtr newNotUniqueErr(const std::string& key) {
    TxnErrorPtr err(new TxnError);
    err->set_err_type(TxnError_ErrType_NOT_UNIQUE);
    err->mutable_not_unique()->set_key(key);
    return err;
}

static TxnErrorPtr newTxnConflictErr(const std::string& expected_txn_id, const std::string& actual_txn_id) {
    TxnErrorPtr err(new TxnError);
    err->set_err_type(TxnError_ErrType_TXN_CONFLICT);
    err->mutable_txn_conflict()->set_expected_txn_id(expected_txn_id);
    err->mutable_txn_conflict()->set_actual_txn_id(actual_txn_id);
    return err;
}

// TODO: load from memory
Status Store::GetTxnValue(const std::string& key, std::string& db_value) {
    return db_->Get(db::CFType::kTxn, key, db_value);
}

Status Store::GetTxnValue(const std::string &key, TxnValue *value) {
    std::string db_value;
    auto s = GetTxnValue(key, db_value);
    if (!s.ok()) {
        return s;
    }
    if (!value->ParseFromString(db_value)) {
        return Status(Status::kCorruption, "parse txn value", EncodeToHex(db_value));
    }
    assert(value->intent().key() == key);
    return Status::OK();
}

Status Store::writeTxnValue(const dspb::TxnValue& value, db::WriteBatch* batch) {
    std::string db_value;
    if (!value.SerializeToString(&db_value)) {
        return Status(Status::kCorruption, "serialize txn value", value.ShortDebugString());
    }
    assert(!value.intent().key().empty());

    FLOG_DEBUG("TXN[{}] put intent: {}", value.txn_id(), value.ShortDebugString());

    auto s = batch->Put(db::CFType::kTxn, value.intent().key(), db_value);
    if (!s.ok()) {
        return Status(Status::kIOError, "put txn value", s.ToString());
    } else {
        return Status::OK();
    }
}

TxnErrorPtr Store::checkLockable(const std::string& key, const std::string& txn_id, bool *exist_flag) {
    TxnValue value;
    auto s = GetTxnValue(key, &value);
    switch (s.code()) {
    case Status::kNotFound:
        return nullptr;
    case Status::kOk:
        assert(value.intent().key() == key);
        if (value.txn_id() == txn_id) {
            *exist_flag = true;
            return nullptr;
        } else {
            return newLockedError(value);
        }
    default:
        return newTxnServerErr(s.code(), s.ToString());
    }
}

// append version & txn_id field after raw value
// {user_value} | {version_value} | {txn_id_value}
static std::string buildTxnDBValue(const std::string& txn_id, uint64_t version, const std::string& raw_value) {
    // encode version column
    std::string version_value;
    EncodeIntValue(&version_value, kVersionColumnID, static_cast<int64_t>(version));

    // encode txn id column (only local txn)
    std::string txn_id_value;
    if (!txn_id.empty()) {
        EncodeBytesValue(&txn_id_value, kTxnIDColumnID, txn_id.c_str(), txn_id.size());
    }

    std::string db_value;
    db_value.reserve(raw_value.size() + txn_id_value.size() + version_value.size());
    // append user value
    db_value.append(raw_value);
    // append version field
    db_value.append(version_value);
    // append txn id value
    if (!txn_id_value.empty()) {
        db_value.append(txn_id_value);
    }

    return db_value;
}

static Status splitTxnDBValue(std::string& value, uint64_t& version, std::string &txn_id) {
    uint32_t col_id = 0;
    EncodeType enc_type;
    size_t tag_offset = 0;
    size_t offset = 0;

    // seek to version column
    for (offset = 0; offset < value.size();) {
        tag_offset = offset;
        if (!DecodeValueTag(value, tag_offset, &col_id, &enc_type)) {
            return Status(Status::kCorruption,
                          std::string("decode value tag failed at offset ") + std::to_string(offset),
                          EncodeToHexString(value));
        }
        if (col_id == kVersionColumnID) {
            break;
        } else { // skip and continue seek
            if (!SkipValue(value, offset)) {
                return Status(Status::kCorruption,
                              std::string("skip value tag failed at offset ") + std::to_string(offset),
                              EncodeToHexString(value));
            }
        }
    }

    auto value_offset = offset;
    // decode version column
    int64_t iversion = 0;
    if (!DecodeIntValue(value, offset, &iversion)) {
        return Status(Status::kCorruption,
                      std::string("decode int value failed at offset ") + std::to_string(offset),
                      EncodeToHexString(value));
    }
    version = static_cast<uint64_t>(iversion);

    // decode txn id column if exist
    if (offset < value.size()) {
        if (!DecodeBytesValue(value, offset, &txn_id)) {
            return Status(Status::kCorruption,
                          std::string("decode txn id failed at offset ") + std::to_string(offset),
                          EncodeToHexString(value));
        }
    }
    // trim version & txn_id column
    value.erase(value_offset);
    return Status::OK();
}

Status Store::getCommittedInfo(const std::string& key, uint64_t& version, std::string& txn_id) {
    std::string db_value;
    auto s = this->Get(key, &db_value);
    if (!s.ok()) {
        return s;
    }
    return splitTxnDBValue(db_value, version, txn_id);
}

TxnErrorPtr Store::checkUniqueAndVersion(const dspb::TxnIntent& intent, const std::string& txn_id, bool local) {
    uint64_t old_version = 0;
    std::string old_txn_id;
    auto s = getCommittedInfo(intent.key(), old_version, old_txn_id);
    if (!s.ok() && s.code() != Status::kNotFound) {
        return newTxnServerErr(s.code(), s.ToString());
    }

    if (intent.typ() == dspb::INSERT) {
        if (old_txn_id == txn_id) {
            return nullptr;
        }
    } else if (intent.typ() == dspb::DELETE) {
        if (s.code() == Status::kNotFound) {
            return nullptr;
        }
    }

    if (intent.check_unique() && s.ok()) {
        return newNotUniqueErr(intent.key());
    }
    if (intent.expected_ver() > 0 && old_version != intent.expected_ver()) {
        return newUnexpectedVerErr(intent.key(), intent.expected_ver(), old_version);
    }
    return nullptr;
}

uint64_t Store::prepareLocal(const dspb::PrepareRequest& req, uint64_t version, dspb::PrepareResponse* resp) {
    assert(req.local());
    auto batch = db_->NewWriteBatch();
    uint64_t bytes_written = 0;
    bool exist_flag = false;
    for (const auto& intent: req.intents()) {
        // check lockable
        auto err = checkLockable(intent.key(), req.txn_id(), &exist_flag);
        if (err != nullptr) {
            resp->add_errors()->Swap(err.get());
            return 0;
        }
        if (exist_flag) {
            setTxnServerErr(resp->add_errors(), Status::kExisted, "lock already exist");
            return 0;
        }

        // check unique and version
        if (intent.check_unique() || intent.expected_ver() != 0) {
            err = checkUniqueAndVersion(intent, req.txn_id(), true);
            if (err != nullptr) {
                resp->add_errors()->Swap(err.get());
                return 0;
            }
        }
        // commit directly
        auto s = commitIntent(intent, version, req.txn_id(), bytes_written, batch.get());
        if (!s.ok()) {
            setTxnServerErr(resp->add_errors(), s.code(), s.ToString());
            return 0;
        }
    }

    auto ret = db_->Write(batch.get(), version);
    if (!ret.ok()) {
        setTxnServerErr(resp->add_errors(), ret.code(), ret.ToString());
        return 0;
    } else {
        addMetricWrite(req.intents_size(), bytes_written);
        return bytes_written;
    }
}

TxnErrorPtr Store::prepareIntent(const PrepareRequest& req, const TxnIntent& intent,
                                 uint64_t version, db::WriteBatch* batch) {
    // check lockable
    bool exist_flag = false;
    auto err = checkLockable(intent.key(), req.txn_id(), &exist_flag);
    if (err != nullptr) {
        return err;
    }
    if (exist_flag) { // lockable, intent is already written
        return nullptr;
    }

    // check unique and version
    if (intent.check_unique() || intent.expected_ver() != 0) {
        err = checkUniqueAndVersion(intent, req.txn_id());
        if (err != nullptr) {
            return err;
        }
    }

    // append to batch
    TxnValue txn_value;
    fillTxnValue(req, intent, version, &txn_value);
    auto s = writeTxnValue(txn_value, batch);
    if (!s.ok()) {
        return newTxnServerErr(s.code(), "serialize txn value failed");
    }
    return nullptr;
}

uint64_t Store::TxnPrepare(const PrepareRequest& req, uint64_t raft_index, PrepareResponse* resp) {
    if (req.local()) {
        return prepareLocal(req, raft_index, resp);
    }

    bool primary_success = true;
    bool fatal_error = false;
    auto batch = db_->NewWriteBatch();
    for (const auto& intent: req.intents()) {
        auto err = prepareIntent(req, intent, raft_index, batch.get());
        if (err != nullptr) {
            if (err->err_type() == TxnError_ErrType_LOCKED) {
                if (intent.is_primary()) {
                    primary_success = false;
                }
            } else {
                resp->clear_errors();
                fatal_error = true;
            }
            resp->add_errors()->Swap(err.get());
        }
        if (fatal_error) break;
    }

    if (!fatal_error && primary_success) {
        auto ret = db_->Write(batch.get(), raft_index);
        if (!ret.ok()) {
            resp->clear_errors();
            setTxnServerErr(resp->add_errors(), ret.code(), ret.ToString());
        }
    }
    return 0;
}

Status Store::commitIntent(const dspb::TxnIntent& intent, uint64_t version,
                    const std::string& txn_id, uint64_t &bytes_written, db::WriteBatch* batch) {
    Status s ;
    switch (intent.typ()) {
        case DELETE:
            s = batch->Delete(intent.key());
            break;
        case INSERT: {
            std::string db_value = buildTxnDBValue(txn_id, version, intent.value());
            s = batch->Put(intent.key(), db_value);
            bytes_written += intent.key().size() + db_value.size();
            break;
        }
        default:
            return Status(Status::kInvalidArgument, "intent type", std::to_string(intent.typ()));
    }
    if (!s.ok()) {
        return Status(Status::kIOError, "commit intent", s.ToString());
    }
    return Status::OK();
}

TxnErrorPtr Store::decidePrimary(const dspb::DecideRequest& req, uint64_t& bytes_written,
                          db::WriteBatch* batch, dspb::DecideResponse* resp) {
    assert(req.is_primary());

    if (req.keys_size() != 1) {
        return newTxnServerErr(Status::kInvalidArgument,
                std::string("invalid key size: ") + std::to_string(req.keys_size()));
    }

    TxnValue value;
    const auto& key = req.keys(0);
    auto s = GetTxnValue(key, &value);
    if (!s.ok()) {
        if (s.code() == Status::kNotFound) {
            return newNotFoundErr(key);
        } else {
            return newTxnServerErr(s.code(), s.ToString());
        }
    }

    FLOG_DEBUG("decide txn primary: get old lock info: {}, req: {}",
            value.ShortDebugString(), req.ShortDebugString());
    // s is ok now
    assert(s.ok());
    if (value.txn_id() != req.txn_id()) {
        return newTxnConflictErr(req.txn_id(), value.txn_id());
    }

    // assign secondary_keys in recover mode
    if (req.recover()) {
        for (const auto& skey: value.secondary_keys()) {
            resp->add_secondary_keys(skey);
        }
    }

    if (value.txn_status() != dspb::TXN_INIT && value.txn_status() != req.status()) {
        return newStatusConflictErr(value.txn_status());
    }

    // txn status is INIT now
    assert(value.txn_status() == dspb::TXN_INIT || value.txn_status() == req.status());
    // update to new status;
    value.set_txn_status(req.status());
    s = writeTxnValue(value, batch);
    if (!s.ok()) {
        return newTxnServerErr(s.code(), s.ToString());
    }
    // commit intent
    if (req.status() == COMMITTED) {
        FLOG_DEBUG("TXN[{}] commit: {}", value.txn_id(), value.ShortDebugString());

        // 2PL don't append txn_id column
        s = commitIntent(value.intent(), value.version(), req.txn_id(), bytes_written, batch);
        if (!s.ok()) {
            return newTxnServerErr(s.code(), s.ToString());
        }
    }

    return nullptr;
}

TxnErrorPtr Store::decideSecondary(const dspb::DecideRequest& req, const std::string& key,
        uint64_t& bytes_written, db::WriteBatch* batch) {
    assert(!req.is_primary());

    TxnValue value;
    auto s = GetTxnValue(key, &value);
    if (!s.ok()) {
        if (s.code() == Status::kNotFound) {
            return nullptr;
        } else {
            return newTxnServerErr(s.code(), s.ToString());
        }
    }
    // s is ok now
    assert(s.ok());
    if (value.txn_id() != req.txn_id()) {
        return nullptr;
    }

    if (req.status() == COMMITTED) {
        FLOG_DEBUG("TXN[{}] commit: {}", value.txn_id(), value.ShortDebugString());

        // 2PL don't append txn_id column
        s = commitIntent(value.intent(), value.version(), req.txn_id(), bytes_written, batch);
        if (!s.ok()) {
            return newTxnServerErr(s.code(), s.ToString());
        }
    }

    FLOG_DEBUG("TXN[{}] delete intent: {}", value.txn_id(), value.ShortDebugString());
    auto ret = batch->Delete(db::CFType::kTxn, value.intent().key());
    if (!ret.ok()) {
        return newTxnServerErr(Status::kIOError, ret.ToString());
    }
    return nullptr;
}

uint64_t Store::TxnDecide(const DecideRequest& req, uint64_t raft_index, DecideResponse* resp) {
    if (req.status() != COMMITTED && req.status() != ABORTED) {
        setTxnServerErr(resp->mutable_err(), Status::kInvalidArgument, "invalid txn status");
        return 0;
    }

    uint64_t bytes_written = 0;
    auto batch = db_->NewWriteBatch();

    TxnErrorPtr err;
    if (req.is_primary()) {
        err = decidePrimary(req, bytes_written, batch.get(), resp);
    } else {
        for (const auto& key: req.keys()) {
            err = decideSecondary(req, key, bytes_written, batch.get());
            if (err != nullptr) {
                break;
            }
        }
    }

    // decide error
    if (err != nullptr) {
        resp->mutable_err()->Swap(err.get());
        return 0;
    }

    auto ret = db_->Write(batch.get(), raft_index);
    if (!ret.ok()) {
        setTxnServerErr(resp->mutable_err(), Status::kIOError, ret.ToString());
        return 0;
    } else {
        addMetricWrite(req.keys_size(), bytes_written);
        return bytes_written;
    }
}

void Store::TxnClearup(const ClearupRequest& req, uint64_t raft_index, ClearupResponse* resp) {
    dspb::TxnValue value;
    auto s = GetTxnValue(req.primary_key(), &value);
    if (!s.ok()) {
        if (s.code() != Status::kNotFound) {
            setTxnServerErr(resp->mutable_err(), s.code(), s.ToString());
            return;
        } else {
            return; // not found, success
        }
    }
    // s is ok now
    if (value.txn_id() != req.txn_id()) { // success
        return;
    }
    if (!value.intent().is_primary()) {
        setTxnServerErr(resp->mutable_err(), Status::kInvalidArgument, "target key is not primary");
        return;
    }

    // delete intent
    FLOG_DEBUG("TXN[{}] delete intent: {}", value.txn_id(), value.ShortDebugString());

    auto ret = db_->Delete(db::CFType::kTxn, req.primary_key(), raft_index);
    if (!ret.ok()) {
        setTxnServerErr(resp->mutable_err(), Status::kIOError, ret.ToString());
        return;
    }
}

void Store::TxnGetLockInfo(const GetLockInfoRequest& req, GetLockInfoResponse* resp) {
    TxnValue value;
    auto ret = GetTxnValue(req.key(), &value);
    if (!ret.ok()) {
        if (ret.code() == Status::kNotFound) {
            setNotFoundErr(resp->mutable_err(), req.key());
        } else {
            setTxnServerErr(resp->mutable_err(), ret.code(), ret.ToString());
        }
        return;
    }
    // ret is ok now
    fillLockInfo(resp->mutable_info(), value);
}

Status Store::TxnSelect(const SelectRequest& req, SelectResponse* resp) {
    // currently aggregation is not supported
    for (int i = 0; i < req.field_list_size(); ++i) {
        auto type = req.field_list(i).typ();
        if (type == dspb::SelectField_Type_AggreFunction) {
            return Status(Status::kNotSupported);
        } else if (type != dspb::SelectField_Type_Column) {
            return Status(Status::kInvalidArgument, "unknown select field type",
                    dspb::SelectField_Type_Name(type));
        }
    }

    RowFetcher fetcher(*this, req);
    Status s;
    bool over = false;
    uint64_t count = 0;
    uint64_t all = 0;
    uint64_t limit = req.has_limit() ? req.limit().count() : kDefaultMaxSelectLimit;
    uint64_t offset = req.has_limit() ? req.limit().offset() : 0;
    while (!over && s.ok()) {
        over = false;
        std::unique_ptr<dspb::Row> row(new dspb::Row);
        s = fetcher.Next(req, *row, over);
        if (s.ok() && !over) {
            ++all;
            if (all > offset) {
                resp->add_rows()->Swap(row.get());
                if (++count >= limit) break;
            }
        }
    }
    resp->set_offset(all);
    return s;
}

Status Store::TxnScan(const dspb::ScanRequest& req, dspb::ScanResponse* resp) {
    int64_t count = 0;
    auto max_count = (req.max_count() == 0) ? static_cast<int64_t>(kDefaultMaxSelectLimit) : req.max_count();
    Status s;
    KvRecord rec;
    auto kv_fetcher = KvFetcher::Create(*this, req);
    while (true) {
        rec.Clear();

        s = kv_fetcher->Next(rec);
        if (!s.ok() || !rec.Valid()) {
            break;
        }
        addMetricRead(1, rec.Size());

        auto kv = resp->add_kvs();
        kv->set_key(std::move(rec.key));

        if (rec.HasValue()) {
            kv->set_value(std::move(rec.value));
        }

        bool has_del_intent = false;
        if (rec.HasIntent()) {
            dspb::TxnValue tv;
            if (!tv.ParseFromString(rec.intent)) {
                return Status(Status::kCorruption, "parse txn value", EncodeToHex(rec.intent));
            }
            has_del_intent = tv.intent().typ() == dspb::DELETE;
            auto resp_intent = kv->mutable_intent();
            resp_intent->set_op_type(tv.intent().typ());
            resp_intent->set_txn_id(tv.txn_id());
            resp_intent->set_primary_key(tv.primary_key());
            // append version column
            resp_intent->set_allocated_value(tv.mutable_intent()->release_value());
            EncodeIntValue(resp_intent->mutable_value(), kVersionColumnID, tv.version());
        }

        if (rec.HasValue() && !has_del_intent) {
            ++count;
        }
        if (count >= max_count) {
            break;
        }
    }
    return s;
}

Status Store::TxnSelectFlow(const dspb::SelectFlowRequest& req, dspb::SelectFlowResponse* resp)
{
    Status s ;
    auto p_count = req.processors_size();
    if (p_count < 1) {
        return Status( Status::kInvalidArgument, " processor size less 1, cur type : ", std::to_string(p_count) );
    }

    // Suppose the first is read data.

    auto processors = req.processors();
    if (processors[0].type() != dspb::TABLE_READ_TYPE &&
        processors[0].type() != dspb::INDEX_READ_TYPE &&
        processors[0].type() != dspb::DATA_SAMPLE_TYPE
    ) {
        return Status( Status::kInvalidArgument, " the first processor type error, cur type : ",
                std::to_string( static_cast<int>(processors[0].type())));
    }

    uint32_t col_num = 0;
    std::vector<int> off_sets;

    dspb::KeyRange range_default;
    range_default.set_start_key(start_key_);
    range_default.set_end_key(end_key_);

    std::unique_ptr<storage::Processor> ptr;
    if (processors[0].type() == dspb::TABLE_READ_TYPE) {
        ptr = std::unique_ptr<Processor>(new storage::TableRead( processors[0].table_read(), range_default, *this ));

        col_num = processors[0].table_read().columns_size();
    } else if (processors[0].type() == dspb::INDEX_READ_TYPE) {
        ptr = std::unique_ptr<Processor>(new storage::IndexRead( processors[0].index_read(), range_default, *this ));
        col_num = processors[0].index_read().columns_size();
    } else if (processors[0].type() == dspb::DATA_SAMPLE_TYPE) {
        ptr = std::unique_ptr<Processor>(new storage::DataSample( processors[0].data_sample(), range_default, *this ));
        col_num = processors[0].data_sample().columns_size();
    } else {
        ; // never to here.
    }

    for (const auto & i : req.output_offsets()) {

        if (i >= col_num ) {
            s = Status( Status::kInvalidArgument,
                    "off_set is overflow",
                    "offset:" + std::to_string(i) + "; num:" + std::to_string(col_num));

            resp->set_code(s.code()); return s;
        }

        off_sets.push_back(i);
    }

    for ( int i = 1 ; i < p_count; i++) {
        switch (processors[i].type()) {
            case dspb::TABLE_READ_TYPE:
            case dspb::INDEX_READ_TYPE:
            case dspb::DATA_SAMPLE_TYPE:
            {
                return Status( Status::kInvalidArgument, " more table_read/index_read processor error ", "" );
            }
            case dspb::SELECTION_TYPE:
            {
                ptr = std::unique_ptr<Processor>(new Selection( processors[i].selection(), std::move(ptr)));
                break;
            }
            case dspb::PROJECTION_TYPE:
            { // Not to care about
                break;
            }
            case dspb::ORDER_BY_TYPE:
            {
                ptr = std::unique_ptr<Processor>(new OrderBy( processors[i].ordering(), std::move(ptr)));
                break;
            }
            case dspb::AGGREGATION_TYPE:
            {
                ptr = std::unique_ptr<Processor>(new Aggregation( processors[i].aggregation(), std::move(ptr)));
                break;
            }
            case dspb::LIMIT_TYPE:
            {
                ptr = std::unique_ptr<Processor>(new Limit( processors[i].limit(), std::move(ptr)));
                break;
            }
            default :
            {
                break;
            }
        }
    }

    while (true) {
        RowResult rowRes;

        s = ptr->next(rowRes);
        if (!s.ok()) {

            if (s.code() == Status::kNoMoreData) {
                s = Status::OK();
            }

            break;
        }

        auto row = resp->add_rows();
        row->set_key(rowRes.GetKey());
        rowRes.EncodeTo( off_sets, ptr->get_col_ids(), row->mutable_value());
        row->mutable_value()->set_version(rowRes.GetVersion());
    }

    if (s.ok()) {
        resp->set_last_key(ptr->get_last_key());
    }

    resp->set_code(s.code());

    return s;
}


} /* namespace storage */
} /* namespace ds */
} /* namespace chubaodb */
