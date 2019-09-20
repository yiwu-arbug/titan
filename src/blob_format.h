#pragma once

#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "table/format.h"
#include "util.h"

namespace rocksdb {
namespace titandb {

// Blob header format:
//
// crc          : fixed32
// size         : fixed32
// compression  : char
const uint64_t kBlobHeaderSize = 9;

// Blob record format:
//
// key          : varint64 length + length bytes
// value        : varint64 length + length bytes
struct BlobRecord {
  Slice key;
  Slice value;

  void EncodeTo(std::string* dst) const;
  Status DecodeFrom(Slice* src);

  size_t size() const { return key.size() + value.size(); }

  friend bool operator==(const BlobRecord& lhs, const BlobRecord& rhs);
};

class BlobEncoder {
 public:
  BlobEncoder(CompressionType compression)
      : compression_ctx_(compression),
        compression_info_(compression_opt_, compression_ctx_,
                          CompressionDict::GetEmptyDict(), compression,
                          0 /*sample_for_compression*/) {}

  void EncodeRecord(const BlobRecord& record);

  Slice GetHeader() const { return Slice(header_, sizeof(header_)); }
  Slice GetRecord() const { return record_; }

  size_t GetEncodedSize() const { return sizeof(header_) + record_.size(); }

 private:
  char header_[kBlobHeaderSize];
  Slice record_;
  std::string record_buffer_;
  std::string compressed_buffer_;
  CompressionOptions compression_opt_;
  CompressionContext compression_ctx_;
  CompressionInfo compression_info_;
};

class BlobDecoder {
 public:
  Status DecodeHeader(Slice* src);
  Status DecodeRecord(Slice* src, BlobRecord* record, OwnedSlice* buffer);

  size_t GetRecordSize() const { return record_size_; }

 private:
  uint32_t crc_{0};
  uint32_t header_crc_{0};
  uint32_t record_size_{0};
  CompressionType compression_{kNoCompression};
};

// Blob handle format:
//
// offset       : varint64
// size         : varint64
struct BlobHandle {
  uint64_t offset{0};
  uint64_t size{0};

  void EncodeTo(std::string* dst) const;
  Status DecodeFrom(Slice* src);

  friend bool operator==(const BlobHandle& lhs, const BlobHandle& rhs);
};

// Blob index format:
//
// type         : char
// file_number_  : varint64
// blob_handle  : varint64 offset + varint64 size
struct BlobIndex {
  enum Type : unsigned char {
    kBlobRecord = 1,
  };
  uint64_t file_number{0};
  BlobHandle blob_handle;

  void EncodeTo(std::string* dst) const;
  Status DecodeFrom(Slice* src);

  friend bool operator==(const BlobIndex& lhs, const BlobIndex& rhs);
};

// Blob file meta format:
//
// file_number_      : varint64
// file_size_        : varint64
class BlobFileMeta {
 public:
  enum class FileEvent {
    kInit,
    kFlushCompleted,
    kCompactionCompleted,
    kGCCompleted,
    kGCBegin,
    kGCOutput,
    kFlushOrCompactionOutput,
    kDbRestart,
    kDelete,
  };

  enum class FileState {
    kInit,  // file never at this state
    kNormal,
    kPendingLSM,  // waiting keys adding to LSM
    kBeingGC,     // being gced
    kPendingGC,   // output of gc, waiting gc finish and keys adding to LSM
    kObsolete,    // already gced, but wait to be physical deleted
  };

  BlobFileMeta() = default;
  BlobFileMeta(uint64_t _file_number, uint64_t _file_size)
      : file_number_(_file_number), file_size_(_file_size) {}

  friend bool operator==(const BlobFileMeta& lhs, const BlobFileMeta& rhs);

  void EncodeTo(std::string* dst) const;
  Status DecodeFrom(Slice* src);

  uint64_t file_number() const { return file_number_; }
  uint64_t file_size() const { return file_size_; }
  FileState file_state() const { return state_; }
  bool is_obsolete() const { return state_ == FileState::kObsolete; }
  uint64_t discardable_size() const { return discardable_size_; }

  bool gc_mark() const { return gc_mark_; }
  void set_gc_mark(bool mark) { gc_mark_ = mark; }

  void FileStateTransit(const FileEvent& event);

  void AddDiscardableSize(uint64_t _discardable_size);
  double GetDiscardableRatio() const;

 private:
  // Persistent field
  uint64_t file_number_{0};
  uint64_t file_size_{0};

  // Not persistent field
  FileState state_{FileState::kInit};

  uint64_t discardable_size_{0};
  // gc_mark is set to true when this file is recovered from re-opening the DB
  // that means this file needs to be checked for GC
  bool gc_mark_{false};
};

// Blob file header format.
// The header is mean to be compatible with header of BlobDB blob files, except
// we use a different magic number.
//
// magic_number         : fixed32
// version              : fixed32
struct BlobFileHeader {
  // The first 32bits from $(echo titandb/blob | sha1sum).
  static const uint32_t kHeaderMagicNumber = 0x2be0a614ul;
  static const uint32_t kVersion1 = 1;
  static const uint64_t kEncodedLength = 4 + 4;

  uint32_t version = kVersion1;

  void EncodeTo(std::string* dst) const;
  Status DecodeFrom(Slice* src);
};

// Blob file footer format:
//
// meta_index_handle    : varint64 offset + varint64 size
// <padding>            : [... kEncodedLength - 12] bytes
// magic_number         : fixed64
// checksum             : fixed32
struct BlobFileFooter {
  // The first 64bits from $(echo titandb/blob | sha1sum).
  static const uint64_t kFooterMagicNumber{0x2be0a6148e39edc6ull};
  static const uint64_t kEncodedLength{BlockHandle::kMaxEncodedLength + 8 + 4};

  BlockHandle meta_index_handle{BlockHandle::NullBlockHandle()};

  void EncodeTo(std::string* dst) const;
  Status DecodeFrom(Slice* src);

  friend bool operator==(const BlobFileFooter& lhs, const BlobFileFooter& rhs);
};

// A convenient template to decode a const slice.
template <typename T>
Status DecodeInto(const Slice& src, T* target) {
  auto tmp = src;
  auto s = target->DecodeFrom(&tmp);
  if (s.ok() && !tmp.empty()) {
    s = Status::Corruption(Slice());
  }
  return s;
}

}  // namespace titandb
}  // namespace rocksdb
