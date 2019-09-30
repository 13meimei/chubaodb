// Copyright 2017 The Cockroach Authors.
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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
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

_Pragma("once");

#include <stdint.h>
#include <string>
#include <vector>
#include <limits>

// from https://github.com/cockroachdb/cockroach

namespace chubaodb {

enum class EncodeType : char {
    Unknown = 0,
    Null,
    NotNull,
    Int,
    Float,
    Decimal,
    Bytes,
    BytesDesc,  // Bytes encoded descendingly
    Time,
    Duration,
    True,
    False,

    SentinelType = 15  // Used in the Value encoding.
};

// Reserved column id for internal use
static const uint32_t kNoColumnID = 0;
static const uint32_t kVersionColumnID = std::numeric_limits<uint32_t>::max();
static const uint32_t kTxnIDColumnID = std::numeric_limits<uint32_t>::max() - 1;
static const EncodeType kVersionColumnType = EncodeType::Int;

void EncodeNonSortingUvarint(std::string* buf, uint64_t value);
size_t NonSortingUvarintSize(uint64_t value);
void EncodeNonSortingVarint(std::string* buf, int64_t value);
size_t NonSortingVarintSize(int64_t value);
bool DecodeNonSortingUvarint(const std::string& data, size_t& offset, uint64_t* value);
bool DecodeNonSortingVarint(const std::string& data, size_t& offset, int64_t* value);

void EncodeUint64Ascending(std::string* buf, uint64_t value);
bool DecodeUint64Ascending(const std::string& data, size_t& offset, uint64_t* value);

void EncodeIntValue(std::string* buf, uint32_t col_id, int64_t value);
size_t IntValueSize(uint32_t col_id, int64_t value);
void EncodeFloatValue(std::string* buf, uint32_t col_id, double value);
void EncodeBytesValue(std::string* buf, uint32_t col_id, const char* value, size_t value_size);
void EncodeNullValue(std::string* buf, uint32_t col_id);

bool DecodeValueTag(const std::string& data, size_t& offset, uint32_t* col_id, EncodeType* type);
bool DecodeIntValue(const std::string& data, size_t& offset, int64_t* value);
bool DecodeFloatValue(const std::string& data, size_t& offset, double* value);
bool DecodeBytesValue(const std::string& data, size_t& offset, std::string* value);
bool SkipValue(const std::string& data, size_t& offset);

void EncodeUvarintAscending(std::string* buf, uint64_t value);
void EncodeUvarintDescending(std::string* buf, uint64_t value);
void EncodeVarintAscending(std::string* buf, int64_t value);
void EncodeFloatAscending(std::string* buf, double value);
void EncodeBytesAscending(std::string* buf, const char* value, size_t value_size);

bool DecodeUvarintAscending(const std::string& buf, size_t& pos, uint64_t* out);
bool DecodeUvarintDescending(const std::string& buf, size_t& pos, uint64_t* out);
bool DecodeVarintAscending(const std::string& buf, size_t& pos, int64_t* out);
bool DecodeFloatAscending(const std::string& buf, size_t& pos, double* out);
bool DecodeBytesAscending(const std::string& buf, size_t& pos, std::string* out);

// for tests or debug
std::string EncodeToHexString(const std::string& str);

size_t VersionColumnSize(int64_t version);

} /* namespace chubaodb */
