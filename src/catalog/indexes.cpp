#include "catalog/indexes.h"

IndexMetadata *IndexMetadata::Create(const index_id_t index_id, const string &index_name,
                                     const table_id_t table_id, const vector<uint32_t> &key_map,
                                     MemHeap *heap) {
  void *buf = heap->Allocate(sizeof(IndexMetadata));
  return new(buf)IndexMetadata(index_id, index_name, table_id, key_map);
}

uint32_t IndexMetadata::SerializeTo(char *buf) const {
  uint32_t offset = 0;
  MACH_WRITE_UINT32(buf, INDEX_METADATA_MAGIC_NUM);
  offset += sizeof(uint32_t);
  MACH_WRITE_TO(index_id_t, buf + offset, index_id_);
  offset += sizeof(index_id_t);
  size_t len = index_name_.length();
  MACH_WRITE_UINT32(buf + offset, len);
  offset += sizeof(size_t);
  memcpy(buf + offset, index_name_.c_str(), index_name_.length());
  offset += len * sizeof(char);
  MACH_WRITE_TO(table_id_t, buf + offset, table_id_);
  offset += sizeof(table_id_t);
  size_t size = key_map_.size();
  MACH_WRITE_UINT32(buf + offset, size);
  offset += sizeof(size_t);
  for (size_t i = 0; i < size; ++ i) {
    MACH_WRITE_UINT32(buf + offset, key_map_[i]);
    offset += sizeof(uint32_t);
  }
  return offset;
}

uint32_t IndexMetadata::GetSerializedSize() const {
  uint32_t offset = 0;
  offset += sizeof(uint32_t);
  offset += sizeof(index_id_t);
  offset += sizeof(table_id_t);
  offset += 2*sizeof(size_t);
  offset += index_name_.length() * sizeof(char);
  offset += key_map_.size() * sizeof(uint32_t);
  return offset;
}

uint32_t IndexMetadata::DeserializeFrom(char *buf, IndexMetadata *&index_meta, MemHeap *heap) {
  uint32_t offset = 0;
  uint32_t magic_num = 0;

  index_id_t index_id;
  table_id_t table_id;
  std::vector<uint32_t> key_map;
  string index_name;
  size_t str_len;

  magic_num = MACH_READ_UINT32(buf+offset);
  if (magic_num != INDEX_METADATA_MAGIC_NUM) return 0;
  offset += sizeof(uint32_t);
  index_id = MACH_READ_FROM(index_id_t, buf + offset);
  offset += sizeof(index_id_t);
  str_len = MACH_READ_FROM(size_t, buf + offset);
  offset += sizeof(size_t);

  char *name1 = new char[str_len];
  memcpy(name1, buf+offset, str_len);
  index_name=name1;
  delete name1;
  offset += str_len;

  table_id = MACH_READ_FROM(table_id_t, buf + offset);
  offset += sizeof(table_id_t);
  size_t size = 0;
  size = MACH_READ_FROM(size_t, buf + offset);
  offset += sizeof(size_t);

  key_map.reserve(size);
  for (size_t i = 0; i < size; ++ i) {
    key_map.push_back(MACH_READ_FROM(uint32_t, buf + offset));
    offset += sizeof(uint32_t);
  }

  index_meta = ALLOC_P(heap,IndexMetadata)(index_id,index_name,table_id,key_map);
  return offset;
}