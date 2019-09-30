// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: dspb/expr.proto

#define INTERNAL_SUPPRESS_PROTOBUF_FIELD_DEPRECATION
#include "dspb/expr.pb.h"

#include <algorithm>

#include <google/protobuf/stubs/common.h>
#include <google/protobuf/stubs/port.h>
#include <google/protobuf/stubs/once.h>
#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/wire_format_lite_inl.h>
#include <google/protobuf/descriptor.h>
#include <google/protobuf/generated_message_reflection.h>
#include <google/protobuf/reflection_ops.h>
#include <google/protobuf/wire_format.h>
// @@protoc_insertion_point(includes)

namespace dspb {
class ColumnInfoDefaultTypeInternal {
public:
 ::google::protobuf::internal::ExplicitlyConstructed<ColumnInfo>
     _instance;
} _ColumnInfo_default_instance_;
class ExprDefaultTypeInternal {
public:
 ::google::protobuf::internal::ExplicitlyConstructed<Expr>
     _instance;
} _Expr_default_instance_;

namespace protobuf_dspb_2fexpr_2eproto {


namespace {

::google::protobuf::Metadata file_level_metadata[2];
const ::google::protobuf::EnumDescriptor* file_level_enum_descriptors[1];

}  // namespace

PROTOBUF_CONSTEXPR_VAR ::google::protobuf::internal::ParseTableField
    const TableStruct::entries[] GOOGLE_ATTRIBUTE_SECTION_VARIABLE(protodesc_cold) = {
  {0, 0, 0, ::google::protobuf::internal::kInvalidMask, 0, 0},
};

PROTOBUF_CONSTEXPR_VAR ::google::protobuf::internal::AuxillaryParseTableField
    const TableStruct::aux[] GOOGLE_ATTRIBUTE_SECTION_VARIABLE(protodesc_cold) = {
  ::google::protobuf::internal::AuxillaryParseTableField(),
};
PROTOBUF_CONSTEXPR_VAR ::google::protobuf::internal::ParseTable const
    TableStruct::schema[] GOOGLE_ATTRIBUTE_SECTION_VARIABLE(protodesc_cold) = {
  { NULL, NULL, 0, -1, -1, -1, -1, NULL, false },
  { NULL, NULL, 0, -1, -1, -1, -1, NULL, false },
};

const ::google::protobuf::uint32 TableStruct::offsets[] GOOGLE_ATTRIBUTE_SECTION_VARIABLE(protodesc_cold) = {
  ~0u,  // no _has_bits_
  GOOGLE_PROTOBUF_GENERATED_MESSAGE_FIELD_OFFSET(ColumnInfo, _internal_metadata_),
  ~0u,  // no _extensions_
  ~0u,  // no _oneof_case_
  ~0u,  // no _weak_field_map_
  GOOGLE_PROTOBUF_GENERATED_MESSAGE_FIELD_OFFSET(ColumnInfo, id_),
  GOOGLE_PROTOBUF_GENERATED_MESSAGE_FIELD_OFFSET(ColumnInfo, typ_),
  GOOGLE_PROTOBUF_GENERATED_MESSAGE_FIELD_OFFSET(ColumnInfo, unsigned__),
  ~0u,  // no _has_bits_
  GOOGLE_PROTOBUF_GENERATED_MESSAGE_FIELD_OFFSET(Expr, _internal_metadata_),
  ~0u,  // no _extensions_
  ~0u,  // no _oneof_case_
  ~0u,  // no _weak_field_map_
  GOOGLE_PROTOBUF_GENERATED_MESSAGE_FIELD_OFFSET(Expr, expr_type_),
  GOOGLE_PROTOBUF_GENERATED_MESSAGE_FIELD_OFFSET(Expr, column_),
  GOOGLE_PROTOBUF_GENERATED_MESSAGE_FIELD_OFFSET(Expr, value_),
  GOOGLE_PROTOBUF_GENERATED_MESSAGE_FIELD_OFFSET(Expr, child_),
};
static const ::google::protobuf::internal::MigrationSchema schemas[] GOOGLE_ATTRIBUTE_SECTION_VARIABLE(protodesc_cold) = {
  { 0, -1, sizeof(ColumnInfo)},
  { 8, -1, sizeof(Expr)},
};

static ::google::protobuf::Message const * const file_default_instances[] = {
  reinterpret_cast<const ::google::protobuf::Message*>(&_ColumnInfo_default_instance_),
  reinterpret_cast<const ::google::protobuf::Message*>(&_Expr_default_instance_),
};

namespace {

void protobuf_AssignDescriptors() {
  AddDescriptors();
  ::google::protobuf::MessageFactory* factory = NULL;
  AssignDescriptors(
      "dspb/expr.proto", schemas, file_default_instances, TableStruct::offsets, factory,
      file_level_metadata, file_level_enum_descriptors, NULL);
}

void protobuf_AssignDescriptorsOnce() {
  static GOOGLE_PROTOBUF_DECLARE_ONCE(once);
  ::google::protobuf::GoogleOnceInit(&once, &protobuf_AssignDescriptors);
}

void protobuf_RegisterTypes(const ::std::string&) GOOGLE_ATTRIBUTE_COLD;
void protobuf_RegisterTypes(const ::std::string&) {
  protobuf_AssignDescriptorsOnce();
  ::google::protobuf::internal::RegisterAllTypes(file_level_metadata, 2);
}

}  // namespace
void TableStruct::InitDefaultsImpl() {
  GOOGLE_PROTOBUF_VERIFY_VERSION;

  ::google::protobuf::internal::InitProtobufDefaults();
  ::basepb::protobuf_basepb_2fbasepb_2eproto::InitDefaults();
  _ColumnInfo_default_instance_._instance.DefaultConstruct();
  ::google::protobuf::internal::OnShutdownDestroyMessage(
      &_ColumnInfo_default_instance_);_Expr_default_instance_._instance.DefaultConstruct();
  ::google::protobuf::internal::OnShutdownDestroyMessage(
      &_Expr_default_instance_);_Expr_default_instance_._instance.get_mutable()->column_ = const_cast< ::dspb::ColumnInfo*>(
      ::dspb::ColumnInfo::internal_default_instance());
}

void InitDefaults() {
  static GOOGLE_PROTOBUF_DECLARE_ONCE(once);
  ::google::protobuf::GoogleOnceInit(&once, &TableStruct::InitDefaultsImpl);
}
namespace {
void AddDescriptorsImpl() {
  InitDefaults();
  static const char descriptor[] GOOGLE_ATTRIBUTE_SECTION_VARIABLE(protodesc_cold) = {
      "\n\017dspb/expr.proto\022\004dspb\032\023basepb/basepb.p"
      "roto\"I\n\nColumnInfo\022\n\n\002id\030\001 \001(\r\022\035\n\003typ\030\002 "
      "\001(\0162\020.basepb.DataType\022\020\n\010unsigned\030\003 \001(\010\""
      "u\n\004Expr\022!\n\texpr_type\030\001 \001(\0162\016.dspb.ExprTy"
      "pe\022 \n\006column\030\002 \001(\0132\020.dspb.ColumnInfo\022\r\n\005"
      "value\030\003 \001(\014\022\031\n\005child\030\004 \003(\0132\n.dspb.Expr*\216"
      "\002\n\010ExprType\022\020\n\014Invalid_Expr\020\000\022\n\n\006Column\020"
      "\001\022\r\n\tConst_Int\020\002\022\016\n\nConst_UInt\020\003\022\020\n\014Cons"
      "t_Double\020\004\022\017\n\013Const_Bytes\020\005\022\014\n\010LogicAnd\020"
      "\n\022\013\n\007LogicOr\020\013\022\014\n\010LogicNot\020\014\022\t\n\005Equal\020\024\022"
      "\014\n\010NotEqual\020\025\022\010\n\004Less\020\026\022\017\n\013LessOrEqual\020\027"
      "\022\n\n\006Larger\020\030\022\021\n\rLargerOrEqual\020\031\022\010\n\004Plus\020"
      "\037\022\t\n\005Minus\020 \022\010\n\004Mult\020!\022\007\n\003Div\020\"b\006proto3"
  };
  ::google::protobuf::DescriptorPool::InternalAddGeneratedFile(
      descriptor, 519);
  ::google::protobuf::MessageFactory::InternalRegisterGeneratedFile(
    "dspb/expr.proto", &protobuf_RegisterTypes);
  ::basepb::protobuf_basepb_2fbasepb_2eproto::AddDescriptors();
}
} // anonymous namespace

void AddDescriptors() {
  static GOOGLE_PROTOBUF_DECLARE_ONCE(once);
  ::google::protobuf::GoogleOnceInit(&once, &AddDescriptorsImpl);
}
// Force AddDescriptors() to be called at dynamic initialization time.
struct StaticDescriptorInitializer {
  StaticDescriptorInitializer() {
    AddDescriptors();
  }
} static_descriptor_initializer;

}  // namespace protobuf_dspb_2fexpr_2eproto

const ::google::protobuf::EnumDescriptor* ExprType_descriptor() {
  protobuf_dspb_2fexpr_2eproto::protobuf_AssignDescriptorsOnce();
  return protobuf_dspb_2fexpr_2eproto::file_level_enum_descriptors[0];
}
bool ExprType_IsValid(int value) {
  switch (value) {
    case 0:
    case 1:
    case 2:
    case 3:
    case 4:
    case 5:
    case 10:
    case 11:
    case 12:
    case 20:
    case 21:
    case 22:
    case 23:
    case 24:
    case 25:
    case 31:
    case 32:
    case 33:
    case 34:
      return true;
    default:
      return false;
  }
}


// ===================================================================

#if !defined(_MSC_VER) || _MSC_VER >= 1900
const int ColumnInfo::kIdFieldNumber;
const int ColumnInfo::kTypFieldNumber;
const int ColumnInfo::kUnsignedFieldNumber;
#endif  // !defined(_MSC_VER) || _MSC_VER >= 1900

ColumnInfo::ColumnInfo()
  : ::google::protobuf::Message(), _internal_metadata_(NULL) {
  if (GOOGLE_PREDICT_TRUE(this != internal_default_instance())) {
    protobuf_dspb_2fexpr_2eproto::InitDefaults();
  }
  SharedCtor();
  // @@protoc_insertion_point(constructor:dspb.ColumnInfo)
}
ColumnInfo::ColumnInfo(const ColumnInfo& from)
  : ::google::protobuf::Message(),
      _internal_metadata_(NULL),
      _cached_size_(0) {
  _internal_metadata_.MergeFrom(from._internal_metadata_);
  ::memcpy(&id_, &from.id_,
    static_cast<size_t>(reinterpret_cast<char*>(&unsigned__) -
    reinterpret_cast<char*>(&id_)) + sizeof(unsigned__));
  // @@protoc_insertion_point(copy_constructor:dspb.ColumnInfo)
}

void ColumnInfo::SharedCtor() {
  ::memset(&id_, 0, static_cast<size_t>(
      reinterpret_cast<char*>(&unsigned__) -
      reinterpret_cast<char*>(&id_)) + sizeof(unsigned__));
  _cached_size_ = 0;
}

ColumnInfo::~ColumnInfo() {
  // @@protoc_insertion_point(destructor:dspb.ColumnInfo)
  SharedDtor();
}

void ColumnInfo::SharedDtor() {
}

void ColumnInfo::SetCachedSize(int size) const {
  GOOGLE_SAFE_CONCURRENT_WRITES_BEGIN();
  _cached_size_ = size;
  GOOGLE_SAFE_CONCURRENT_WRITES_END();
}
const ::google::protobuf::Descriptor* ColumnInfo::descriptor() {
  protobuf_dspb_2fexpr_2eproto::protobuf_AssignDescriptorsOnce();
  return protobuf_dspb_2fexpr_2eproto::file_level_metadata[kIndexInFileMessages].descriptor;
}

const ColumnInfo& ColumnInfo::default_instance() {
  protobuf_dspb_2fexpr_2eproto::InitDefaults();
  return *internal_default_instance();
}

ColumnInfo* ColumnInfo::New(::google::protobuf::Arena* arena) const {
  ColumnInfo* n = new ColumnInfo;
  if (arena != NULL) {
    arena->Own(n);
  }
  return n;
}

void ColumnInfo::Clear() {
// @@protoc_insertion_point(message_clear_start:dspb.ColumnInfo)
  ::google::protobuf::uint32 cached_has_bits = 0;
  // Prevent compiler warnings about cached_has_bits being unused
  (void) cached_has_bits;

  ::memset(&id_, 0, static_cast<size_t>(
      reinterpret_cast<char*>(&unsigned__) -
      reinterpret_cast<char*>(&id_)) + sizeof(unsigned__));
  _internal_metadata_.Clear();
}

bool ColumnInfo::MergePartialFromCodedStream(
    ::google::protobuf::io::CodedInputStream* input) {
#define DO_(EXPRESSION) if (!GOOGLE_PREDICT_TRUE(EXPRESSION)) goto failure
  ::google::protobuf::uint32 tag;
  // @@protoc_insertion_point(parse_start:dspb.ColumnInfo)
  for (;;) {
    ::std::pair< ::google::protobuf::uint32, bool> p = input->ReadTagWithCutoffNoLastTag(127u);
    tag = p.first;
    if (!p.second) goto handle_unusual;
    switch (::google::protobuf::internal::WireFormatLite::GetTagFieldNumber(tag)) {
      // uint32 id = 1;
      case 1: {
        if (static_cast< ::google::protobuf::uint8>(tag) ==
            static_cast< ::google::protobuf::uint8>(8u /* 8 & 0xFF */)) {

          DO_((::google::protobuf::internal::WireFormatLite::ReadPrimitive<
                   ::google::protobuf::uint32, ::google::protobuf::internal::WireFormatLite::TYPE_UINT32>(
                 input, &id_)));
        } else {
          goto handle_unusual;
        }
        break;
      }

      // .basepb.DataType typ = 2;
      case 2: {
        if (static_cast< ::google::protobuf::uint8>(tag) ==
            static_cast< ::google::protobuf::uint8>(16u /* 16 & 0xFF */)) {
          int value;
          DO_((::google::protobuf::internal::WireFormatLite::ReadPrimitive<
                   int, ::google::protobuf::internal::WireFormatLite::TYPE_ENUM>(
                 input, &value)));
          set_typ(static_cast< ::basepb::DataType >(value));
        } else {
          goto handle_unusual;
        }
        break;
      }

      // bool unsigned = 3;
      case 3: {
        if (static_cast< ::google::protobuf::uint8>(tag) ==
            static_cast< ::google::protobuf::uint8>(24u /* 24 & 0xFF */)) {

          DO_((::google::protobuf::internal::WireFormatLite::ReadPrimitive<
                   bool, ::google::protobuf::internal::WireFormatLite::TYPE_BOOL>(
                 input, &unsigned__)));
        } else {
          goto handle_unusual;
        }
        break;
      }

      default: {
      handle_unusual:
        if (tag == 0) {
          goto success;
        }
        DO_(::google::protobuf::internal::WireFormat::SkipField(
              input, tag, _internal_metadata_.mutable_unknown_fields()));
        break;
      }
    }
  }
success:
  // @@protoc_insertion_point(parse_success:dspb.ColumnInfo)
  return true;
failure:
  // @@protoc_insertion_point(parse_failure:dspb.ColumnInfo)
  return false;
#undef DO_
}

void ColumnInfo::SerializeWithCachedSizes(
    ::google::protobuf::io::CodedOutputStream* output) const {
  // @@protoc_insertion_point(serialize_start:dspb.ColumnInfo)
  ::google::protobuf::uint32 cached_has_bits = 0;
  (void) cached_has_bits;

  // uint32 id = 1;
  if (this->id() != 0) {
    ::google::protobuf::internal::WireFormatLite::WriteUInt32(1, this->id(), output);
  }

  // .basepb.DataType typ = 2;
  if (this->typ() != 0) {
    ::google::protobuf::internal::WireFormatLite::WriteEnum(
      2, this->typ(), output);
  }

  // bool unsigned = 3;
  if (this->unsigned_() != 0) {
    ::google::protobuf::internal::WireFormatLite::WriteBool(3, this->unsigned_(), output);
  }

  if ((_internal_metadata_.have_unknown_fields() &&  ::google::protobuf::internal::GetProto3PreserveUnknownsDefault())) {
    ::google::protobuf::internal::WireFormat::SerializeUnknownFields(
        (::google::protobuf::internal::GetProto3PreserveUnknownsDefault()   ? _internal_metadata_.unknown_fields()   : _internal_metadata_.default_instance()), output);
  }
  // @@protoc_insertion_point(serialize_end:dspb.ColumnInfo)
}

::google::protobuf::uint8* ColumnInfo::InternalSerializeWithCachedSizesToArray(
    bool deterministic, ::google::protobuf::uint8* target) const {
  (void)deterministic; // Unused
  // @@protoc_insertion_point(serialize_to_array_start:dspb.ColumnInfo)
  ::google::protobuf::uint32 cached_has_bits = 0;
  (void) cached_has_bits;

  // uint32 id = 1;
  if (this->id() != 0) {
    target = ::google::protobuf::internal::WireFormatLite::WriteUInt32ToArray(1, this->id(), target);
  }

  // .basepb.DataType typ = 2;
  if (this->typ() != 0) {
    target = ::google::protobuf::internal::WireFormatLite::WriteEnumToArray(
      2, this->typ(), target);
  }

  // bool unsigned = 3;
  if (this->unsigned_() != 0) {
    target = ::google::protobuf::internal::WireFormatLite::WriteBoolToArray(3, this->unsigned_(), target);
  }

  if ((_internal_metadata_.have_unknown_fields() &&  ::google::protobuf::internal::GetProto3PreserveUnknownsDefault())) {
    target = ::google::protobuf::internal::WireFormat::SerializeUnknownFieldsToArray(
        (::google::protobuf::internal::GetProto3PreserveUnknownsDefault()   ? _internal_metadata_.unknown_fields()   : _internal_metadata_.default_instance()), target);
  }
  // @@protoc_insertion_point(serialize_to_array_end:dspb.ColumnInfo)
  return target;
}

size_t ColumnInfo::ByteSizeLong() const {
// @@protoc_insertion_point(message_byte_size_start:dspb.ColumnInfo)
  size_t total_size = 0;

  if ((_internal_metadata_.have_unknown_fields() &&  ::google::protobuf::internal::GetProto3PreserveUnknownsDefault())) {
    total_size +=
      ::google::protobuf::internal::WireFormat::ComputeUnknownFieldsSize(
        (::google::protobuf::internal::GetProto3PreserveUnknownsDefault()   ? _internal_metadata_.unknown_fields()   : _internal_metadata_.default_instance()));
  }
  // uint32 id = 1;
  if (this->id() != 0) {
    total_size += 1 +
      ::google::protobuf::internal::WireFormatLite::UInt32Size(
        this->id());
  }

  // .basepb.DataType typ = 2;
  if (this->typ() != 0) {
    total_size += 1 +
      ::google::protobuf::internal::WireFormatLite::EnumSize(this->typ());
  }

  // bool unsigned = 3;
  if (this->unsigned_() != 0) {
    total_size += 1 + 1;
  }

  int cached_size = ::google::protobuf::internal::ToCachedSize(total_size);
  GOOGLE_SAFE_CONCURRENT_WRITES_BEGIN();
  _cached_size_ = cached_size;
  GOOGLE_SAFE_CONCURRENT_WRITES_END();
  return total_size;
}

void ColumnInfo::MergeFrom(const ::google::protobuf::Message& from) {
// @@protoc_insertion_point(generalized_merge_from_start:dspb.ColumnInfo)
  GOOGLE_DCHECK_NE(&from, this);
  const ColumnInfo* source =
      ::google::protobuf::internal::DynamicCastToGenerated<const ColumnInfo>(
          &from);
  if (source == NULL) {
  // @@protoc_insertion_point(generalized_merge_from_cast_fail:dspb.ColumnInfo)
    ::google::protobuf::internal::ReflectionOps::Merge(from, this);
  } else {
  // @@protoc_insertion_point(generalized_merge_from_cast_success:dspb.ColumnInfo)
    MergeFrom(*source);
  }
}

void ColumnInfo::MergeFrom(const ColumnInfo& from) {
// @@protoc_insertion_point(class_specific_merge_from_start:dspb.ColumnInfo)
  GOOGLE_DCHECK_NE(&from, this);
  _internal_metadata_.MergeFrom(from._internal_metadata_);
  ::google::protobuf::uint32 cached_has_bits = 0;
  (void) cached_has_bits;

  if (from.id() != 0) {
    set_id(from.id());
  }
  if (from.typ() != 0) {
    set_typ(from.typ());
  }
  if (from.unsigned_() != 0) {
    set_unsigned_(from.unsigned_());
  }
}

void ColumnInfo::CopyFrom(const ::google::protobuf::Message& from) {
// @@protoc_insertion_point(generalized_copy_from_start:dspb.ColumnInfo)
  if (&from == this) return;
  Clear();
  MergeFrom(from);
}

void ColumnInfo::CopyFrom(const ColumnInfo& from) {
// @@protoc_insertion_point(class_specific_copy_from_start:dspb.ColumnInfo)
  if (&from == this) return;
  Clear();
  MergeFrom(from);
}

bool ColumnInfo::IsInitialized() const {
  return true;
}

void ColumnInfo::Swap(ColumnInfo* other) {
  if (other == this) return;
  InternalSwap(other);
}
void ColumnInfo::InternalSwap(ColumnInfo* other) {
  using std::swap;
  swap(id_, other->id_);
  swap(typ_, other->typ_);
  swap(unsigned__, other->unsigned__);
  _internal_metadata_.Swap(&other->_internal_metadata_);
  swap(_cached_size_, other->_cached_size_);
}

::google::protobuf::Metadata ColumnInfo::GetMetadata() const {
  protobuf_dspb_2fexpr_2eproto::protobuf_AssignDescriptorsOnce();
  return protobuf_dspb_2fexpr_2eproto::file_level_metadata[kIndexInFileMessages];
}

#if PROTOBUF_INLINE_NOT_IN_HEADERS
// ColumnInfo

// uint32 id = 1;
void ColumnInfo::clear_id() {
  id_ = 0u;
}
::google::protobuf::uint32 ColumnInfo::id() const {
  // @@protoc_insertion_point(field_get:dspb.ColumnInfo.id)
  return id_;
}
void ColumnInfo::set_id(::google::protobuf::uint32 value) {
  
  id_ = value;
  // @@protoc_insertion_point(field_set:dspb.ColumnInfo.id)
}

// .basepb.DataType typ = 2;
void ColumnInfo::clear_typ() {
  typ_ = 0;
}
::basepb::DataType ColumnInfo::typ() const {
  // @@protoc_insertion_point(field_get:dspb.ColumnInfo.typ)
  return static_cast< ::basepb::DataType >(typ_);
}
void ColumnInfo::set_typ(::basepb::DataType value) {
  
  typ_ = value;
  // @@protoc_insertion_point(field_set:dspb.ColumnInfo.typ)
}

// bool unsigned = 3;
void ColumnInfo::clear_unsigned_() {
  unsigned__ = false;
}
bool ColumnInfo::unsigned_() const {
  // @@protoc_insertion_point(field_get:dspb.ColumnInfo.unsigned)
  return unsigned__;
}
void ColumnInfo::set_unsigned_(bool value) {
  
  unsigned__ = value;
  // @@protoc_insertion_point(field_set:dspb.ColumnInfo.unsigned)
}

#endif  // PROTOBUF_INLINE_NOT_IN_HEADERS

// ===================================================================

#if !defined(_MSC_VER) || _MSC_VER >= 1900
const int Expr::kExprTypeFieldNumber;
const int Expr::kColumnFieldNumber;
const int Expr::kValueFieldNumber;
const int Expr::kChildFieldNumber;
#endif  // !defined(_MSC_VER) || _MSC_VER >= 1900

Expr::Expr()
  : ::google::protobuf::Message(), _internal_metadata_(NULL) {
  if (GOOGLE_PREDICT_TRUE(this != internal_default_instance())) {
    protobuf_dspb_2fexpr_2eproto::InitDefaults();
  }
  SharedCtor();
  // @@protoc_insertion_point(constructor:dspb.Expr)
}
Expr::Expr(const Expr& from)
  : ::google::protobuf::Message(),
      _internal_metadata_(NULL),
      child_(from.child_),
      _cached_size_(0) {
  _internal_metadata_.MergeFrom(from._internal_metadata_);
  value_.UnsafeSetDefault(&::google::protobuf::internal::GetEmptyStringAlreadyInited());
  if (from.value().size() > 0) {
    value_.AssignWithDefault(&::google::protobuf::internal::GetEmptyStringAlreadyInited(), from.value_);
  }
  if (from.has_column()) {
    column_ = new ::dspb::ColumnInfo(*from.column_);
  } else {
    column_ = NULL;
  }
  expr_type_ = from.expr_type_;
  // @@protoc_insertion_point(copy_constructor:dspb.Expr)
}

void Expr::SharedCtor() {
  value_.UnsafeSetDefault(&::google::protobuf::internal::GetEmptyStringAlreadyInited());
  ::memset(&column_, 0, static_cast<size_t>(
      reinterpret_cast<char*>(&expr_type_) -
      reinterpret_cast<char*>(&column_)) + sizeof(expr_type_));
  _cached_size_ = 0;
}

Expr::~Expr() {
  // @@protoc_insertion_point(destructor:dspb.Expr)
  SharedDtor();
}

void Expr::SharedDtor() {
  value_.DestroyNoArena(&::google::protobuf::internal::GetEmptyStringAlreadyInited());
  if (this != internal_default_instance()) delete column_;
}

void Expr::SetCachedSize(int size) const {
  GOOGLE_SAFE_CONCURRENT_WRITES_BEGIN();
  _cached_size_ = size;
  GOOGLE_SAFE_CONCURRENT_WRITES_END();
}
const ::google::protobuf::Descriptor* Expr::descriptor() {
  protobuf_dspb_2fexpr_2eproto::protobuf_AssignDescriptorsOnce();
  return protobuf_dspb_2fexpr_2eproto::file_level_metadata[kIndexInFileMessages].descriptor;
}

const Expr& Expr::default_instance() {
  protobuf_dspb_2fexpr_2eproto::InitDefaults();
  return *internal_default_instance();
}

Expr* Expr::New(::google::protobuf::Arena* arena) const {
  Expr* n = new Expr;
  if (arena != NULL) {
    arena->Own(n);
  }
  return n;
}

void Expr::Clear() {
// @@protoc_insertion_point(message_clear_start:dspb.Expr)
  ::google::protobuf::uint32 cached_has_bits = 0;
  // Prevent compiler warnings about cached_has_bits being unused
  (void) cached_has_bits;

  child_.Clear();
  value_.ClearToEmptyNoArena(&::google::protobuf::internal::GetEmptyStringAlreadyInited());
  if (GetArenaNoVirtual() == NULL && column_ != NULL) {
    delete column_;
  }
  column_ = NULL;
  expr_type_ = 0;
  _internal_metadata_.Clear();
}

bool Expr::MergePartialFromCodedStream(
    ::google::protobuf::io::CodedInputStream* input) {
#define DO_(EXPRESSION) if (!GOOGLE_PREDICT_TRUE(EXPRESSION)) goto failure
  ::google::protobuf::uint32 tag;
  // @@protoc_insertion_point(parse_start:dspb.Expr)
  for (;;) {
    ::std::pair< ::google::protobuf::uint32, bool> p = input->ReadTagWithCutoffNoLastTag(127u);
    tag = p.first;
    if (!p.second) goto handle_unusual;
    switch (::google::protobuf::internal::WireFormatLite::GetTagFieldNumber(tag)) {
      // .dspb.ExprType expr_type = 1;
      case 1: {
        if (static_cast< ::google::protobuf::uint8>(tag) ==
            static_cast< ::google::protobuf::uint8>(8u /* 8 & 0xFF */)) {
          int value;
          DO_((::google::protobuf::internal::WireFormatLite::ReadPrimitive<
                   int, ::google::protobuf::internal::WireFormatLite::TYPE_ENUM>(
                 input, &value)));
          set_expr_type(static_cast< ::dspb::ExprType >(value));
        } else {
          goto handle_unusual;
        }
        break;
      }

      // .dspb.ColumnInfo column = 2;
      case 2: {
        if (static_cast< ::google::protobuf::uint8>(tag) ==
            static_cast< ::google::protobuf::uint8>(18u /* 18 & 0xFF */)) {
          DO_(::google::protobuf::internal::WireFormatLite::ReadMessageNoVirtual(
               input, mutable_column()));
        } else {
          goto handle_unusual;
        }
        break;
      }

      // bytes value = 3;
      case 3: {
        if (static_cast< ::google::protobuf::uint8>(tag) ==
            static_cast< ::google::protobuf::uint8>(26u /* 26 & 0xFF */)) {
          DO_(::google::protobuf::internal::WireFormatLite::ReadBytes(
                input, this->mutable_value()));
        } else {
          goto handle_unusual;
        }
        break;
      }

      // repeated .dspb.Expr child = 4;
      case 4: {
        if (static_cast< ::google::protobuf::uint8>(tag) ==
            static_cast< ::google::protobuf::uint8>(34u /* 34 & 0xFF */)) {
          DO_(::google::protobuf::internal::WireFormatLite::ReadMessageNoVirtual(
                input, add_child()));
        } else {
          goto handle_unusual;
        }
        break;
      }

      default: {
      handle_unusual:
        if (tag == 0) {
          goto success;
        }
        DO_(::google::protobuf::internal::WireFormat::SkipField(
              input, tag, _internal_metadata_.mutable_unknown_fields()));
        break;
      }
    }
  }
success:
  // @@protoc_insertion_point(parse_success:dspb.Expr)
  return true;
failure:
  // @@protoc_insertion_point(parse_failure:dspb.Expr)
  return false;
#undef DO_
}

void Expr::SerializeWithCachedSizes(
    ::google::protobuf::io::CodedOutputStream* output) const {
  // @@protoc_insertion_point(serialize_start:dspb.Expr)
  ::google::protobuf::uint32 cached_has_bits = 0;
  (void) cached_has_bits;

  // .dspb.ExprType expr_type = 1;
  if (this->expr_type() != 0) {
    ::google::protobuf::internal::WireFormatLite::WriteEnum(
      1, this->expr_type(), output);
  }

  // .dspb.ColumnInfo column = 2;
  if (this->has_column()) {
    ::google::protobuf::internal::WireFormatLite::WriteMessageMaybeToArray(
      2, *this->column_, output);
  }

  // bytes value = 3;
  if (this->value().size() > 0) {
    ::google::protobuf::internal::WireFormatLite::WriteBytesMaybeAliased(
      3, this->value(), output);
  }

  // repeated .dspb.Expr child = 4;
  for (unsigned int i = 0,
      n = static_cast<unsigned int>(this->child_size()); i < n; i++) {
    ::google::protobuf::internal::WireFormatLite::WriteMessageMaybeToArray(
      4, this->child(static_cast<int>(i)), output);
  }

  if ((_internal_metadata_.have_unknown_fields() &&  ::google::protobuf::internal::GetProto3PreserveUnknownsDefault())) {
    ::google::protobuf::internal::WireFormat::SerializeUnknownFields(
        (::google::protobuf::internal::GetProto3PreserveUnknownsDefault()   ? _internal_metadata_.unknown_fields()   : _internal_metadata_.default_instance()), output);
  }
  // @@protoc_insertion_point(serialize_end:dspb.Expr)
}

::google::protobuf::uint8* Expr::InternalSerializeWithCachedSizesToArray(
    bool deterministic, ::google::protobuf::uint8* target) const {
  (void)deterministic; // Unused
  // @@protoc_insertion_point(serialize_to_array_start:dspb.Expr)
  ::google::protobuf::uint32 cached_has_bits = 0;
  (void) cached_has_bits;

  // .dspb.ExprType expr_type = 1;
  if (this->expr_type() != 0) {
    target = ::google::protobuf::internal::WireFormatLite::WriteEnumToArray(
      1, this->expr_type(), target);
  }

  // .dspb.ColumnInfo column = 2;
  if (this->has_column()) {
    target = ::google::protobuf::internal::WireFormatLite::
      InternalWriteMessageNoVirtualToArray(
        2, *this->column_, deterministic, target);
  }

  // bytes value = 3;
  if (this->value().size() > 0) {
    target =
      ::google::protobuf::internal::WireFormatLite::WriteBytesToArray(
        3, this->value(), target);
  }

  // repeated .dspb.Expr child = 4;
  for (unsigned int i = 0,
      n = static_cast<unsigned int>(this->child_size()); i < n; i++) {
    target = ::google::protobuf::internal::WireFormatLite::
      InternalWriteMessageNoVirtualToArray(
        4, this->child(static_cast<int>(i)), deterministic, target);
  }

  if ((_internal_metadata_.have_unknown_fields() &&  ::google::protobuf::internal::GetProto3PreserveUnknownsDefault())) {
    target = ::google::protobuf::internal::WireFormat::SerializeUnknownFieldsToArray(
        (::google::protobuf::internal::GetProto3PreserveUnknownsDefault()   ? _internal_metadata_.unknown_fields()   : _internal_metadata_.default_instance()), target);
  }
  // @@protoc_insertion_point(serialize_to_array_end:dspb.Expr)
  return target;
}

size_t Expr::ByteSizeLong() const {
// @@protoc_insertion_point(message_byte_size_start:dspb.Expr)
  size_t total_size = 0;

  if ((_internal_metadata_.have_unknown_fields() &&  ::google::protobuf::internal::GetProto3PreserveUnknownsDefault())) {
    total_size +=
      ::google::protobuf::internal::WireFormat::ComputeUnknownFieldsSize(
        (::google::protobuf::internal::GetProto3PreserveUnknownsDefault()   ? _internal_metadata_.unknown_fields()   : _internal_metadata_.default_instance()));
  }
  // repeated .dspb.Expr child = 4;
  {
    unsigned int count = static_cast<unsigned int>(this->child_size());
    total_size += 1UL * count;
    for (unsigned int i = 0; i < count; i++) {
      total_size +=
        ::google::protobuf::internal::WireFormatLite::MessageSizeNoVirtual(
          this->child(static_cast<int>(i)));
    }
  }

  // bytes value = 3;
  if (this->value().size() > 0) {
    total_size += 1 +
      ::google::protobuf::internal::WireFormatLite::BytesSize(
        this->value());
  }

  // .dspb.ColumnInfo column = 2;
  if (this->has_column()) {
    total_size += 1 +
      ::google::protobuf::internal::WireFormatLite::MessageSizeNoVirtual(
        *this->column_);
  }

  // .dspb.ExprType expr_type = 1;
  if (this->expr_type() != 0) {
    total_size += 1 +
      ::google::protobuf::internal::WireFormatLite::EnumSize(this->expr_type());
  }

  int cached_size = ::google::protobuf::internal::ToCachedSize(total_size);
  GOOGLE_SAFE_CONCURRENT_WRITES_BEGIN();
  _cached_size_ = cached_size;
  GOOGLE_SAFE_CONCURRENT_WRITES_END();
  return total_size;
}

void Expr::MergeFrom(const ::google::protobuf::Message& from) {
// @@protoc_insertion_point(generalized_merge_from_start:dspb.Expr)
  GOOGLE_DCHECK_NE(&from, this);
  const Expr* source =
      ::google::protobuf::internal::DynamicCastToGenerated<const Expr>(
          &from);
  if (source == NULL) {
  // @@protoc_insertion_point(generalized_merge_from_cast_fail:dspb.Expr)
    ::google::protobuf::internal::ReflectionOps::Merge(from, this);
  } else {
  // @@protoc_insertion_point(generalized_merge_from_cast_success:dspb.Expr)
    MergeFrom(*source);
  }
}

void Expr::MergeFrom(const Expr& from) {
// @@protoc_insertion_point(class_specific_merge_from_start:dspb.Expr)
  GOOGLE_DCHECK_NE(&from, this);
  _internal_metadata_.MergeFrom(from._internal_metadata_);
  ::google::protobuf::uint32 cached_has_bits = 0;
  (void) cached_has_bits;

  child_.MergeFrom(from.child_);
  if (from.value().size() > 0) {

    value_.AssignWithDefault(&::google::protobuf::internal::GetEmptyStringAlreadyInited(), from.value_);
  }
  if (from.has_column()) {
    mutable_column()->::dspb::ColumnInfo::MergeFrom(from.column());
  }
  if (from.expr_type() != 0) {
    set_expr_type(from.expr_type());
  }
}

void Expr::CopyFrom(const ::google::protobuf::Message& from) {
// @@protoc_insertion_point(generalized_copy_from_start:dspb.Expr)
  if (&from == this) return;
  Clear();
  MergeFrom(from);
}

void Expr::CopyFrom(const Expr& from) {
// @@protoc_insertion_point(class_specific_copy_from_start:dspb.Expr)
  if (&from == this) return;
  Clear();
  MergeFrom(from);
}

bool Expr::IsInitialized() const {
  return true;
}

void Expr::Swap(Expr* other) {
  if (other == this) return;
  InternalSwap(other);
}
void Expr::InternalSwap(Expr* other) {
  using std::swap;
  child_.InternalSwap(&other->child_);
  value_.Swap(&other->value_);
  swap(column_, other->column_);
  swap(expr_type_, other->expr_type_);
  _internal_metadata_.Swap(&other->_internal_metadata_);
  swap(_cached_size_, other->_cached_size_);
}

::google::protobuf::Metadata Expr::GetMetadata() const {
  protobuf_dspb_2fexpr_2eproto::protobuf_AssignDescriptorsOnce();
  return protobuf_dspb_2fexpr_2eproto::file_level_metadata[kIndexInFileMessages];
}

#if PROTOBUF_INLINE_NOT_IN_HEADERS
// Expr

// .dspb.ExprType expr_type = 1;
void Expr::clear_expr_type() {
  expr_type_ = 0;
}
::dspb::ExprType Expr::expr_type() const {
  // @@protoc_insertion_point(field_get:dspb.Expr.expr_type)
  return static_cast< ::dspb::ExprType >(expr_type_);
}
void Expr::set_expr_type(::dspb::ExprType value) {
  
  expr_type_ = value;
  // @@protoc_insertion_point(field_set:dspb.Expr.expr_type)
}

// .dspb.ColumnInfo column = 2;
bool Expr::has_column() const {
  return this != internal_default_instance() && column_ != NULL;
}
void Expr::clear_column() {
  if (GetArenaNoVirtual() == NULL && column_ != NULL) delete column_;
  column_ = NULL;
}
const ::dspb::ColumnInfo& Expr::column() const {
  const ::dspb::ColumnInfo* p = column_;
  // @@protoc_insertion_point(field_get:dspb.Expr.column)
  return p != NULL ? *p : *reinterpret_cast<const ::dspb::ColumnInfo*>(
      &::dspb::_ColumnInfo_default_instance_);
}
::dspb::ColumnInfo* Expr::mutable_column() {
  
  if (column_ == NULL) {
    column_ = new ::dspb::ColumnInfo;
  }
  // @@protoc_insertion_point(field_mutable:dspb.Expr.column)
  return column_;
}
::dspb::ColumnInfo* Expr::release_column() {
  // @@protoc_insertion_point(field_release:dspb.Expr.column)
  
  ::dspb::ColumnInfo* temp = column_;
  column_ = NULL;
  return temp;
}
void Expr::set_allocated_column(::dspb::ColumnInfo* column) {
  delete column_;
  column_ = column;
  if (column) {
    
  } else {
    
  }
  // @@protoc_insertion_point(field_set_allocated:dspb.Expr.column)
}

// bytes value = 3;
void Expr::clear_value() {
  value_.ClearToEmptyNoArena(&::google::protobuf::internal::GetEmptyStringAlreadyInited());
}
const ::std::string& Expr::value() const {
  // @@protoc_insertion_point(field_get:dspb.Expr.value)
  return value_.GetNoArena();
}
void Expr::set_value(const ::std::string& value) {
  
  value_.SetNoArena(&::google::protobuf::internal::GetEmptyStringAlreadyInited(), value);
  // @@protoc_insertion_point(field_set:dspb.Expr.value)
}
#if LANG_CXX11
void Expr::set_value(::std::string&& value) {
  
  value_.SetNoArena(
    &::google::protobuf::internal::GetEmptyStringAlreadyInited(), ::std::move(value));
  // @@protoc_insertion_point(field_set_rvalue:dspb.Expr.value)
}
#endif
void Expr::set_value(const char* value) {
  GOOGLE_DCHECK(value != NULL);
  
  value_.SetNoArena(&::google::protobuf::internal::GetEmptyStringAlreadyInited(), ::std::string(value));
  // @@protoc_insertion_point(field_set_char:dspb.Expr.value)
}
void Expr::set_value(const void* value, size_t size) {
  
  value_.SetNoArena(&::google::protobuf::internal::GetEmptyStringAlreadyInited(),
      ::std::string(reinterpret_cast<const char*>(value), size));
  // @@protoc_insertion_point(field_set_pointer:dspb.Expr.value)
}
::std::string* Expr::mutable_value() {
  
  // @@protoc_insertion_point(field_mutable:dspb.Expr.value)
  return value_.MutableNoArena(&::google::protobuf::internal::GetEmptyStringAlreadyInited());
}
::std::string* Expr::release_value() {
  // @@protoc_insertion_point(field_release:dspb.Expr.value)
  
  return value_.ReleaseNoArena(&::google::protobuf::internal::GetEmptyStringAlreadyInited());
}
void Expr::set_allocated_value(::std::string* value) {
  if (value != NULL) {
    
  } else {
    
  }
  value_.SetAllocatedNoArena(&::google::protobuf::internal::GetEmptyStringAlreadyInited(), value);
  // @@protoc_insertion_point(field_set_allocated:dspb.Expr.value)
}

// repeated .dspb.Expr child = 4;
int Expr::child_size() const {
  return child_.size();
}
void Expr::clear_child() {
  child_.Clear();
}
const ::dspb::Expr& Expr::child(int index) const {
  // @@protoc_insertion_point(field_get:dspb.Expr.child)
  return child_.Get(index);
}
::dspb::Expr* Expr::mutable_child(int index) {
  // @@protoc_insertion_point(field_mutable:dspb.Expr.child)
  return child_.Mutable(index);
}
::dspb::Expr* Expr::add_child() {
  // @@protoc_insertion_point(field_add:dspb.Expr.child)
  return child_.Add();
}
::google::protobuf::RepeatedPtrField< ::dspb::Expr >*
Expr::mutable_child() {
  // @@protoc_insertion_point(field_mutable_list:dspb.Expr.child)
  return &child_;
}
const ::google::protobuf::RepeatedPtrField< ::dspb::Expr >&
Expr::child() const {
  // @@protoc_insertion_point(field_list:dspb.Expr.child)
  return child_;
}

#endif  // PROTOBUF_INLINE_NOT_IN_HEADERS

// @@protoc_insertion_point(namespace_scope)

}  // namespace dspb

// @@protoc_insertion_point(global_scope)
