#include "catalog/table.h"
#include<iostream>

uint32_t TableMetadata::SerializeTo(char *buf) const {
  uint32_t tot_offset = 0;
  MACH_WRITE_TO(uint32_t, buf+tot_offset, TABLE_METADATA_MAGIC_NUM);
  tot_offset += sizeof(uint32_t);
  MACH_WRITE_TO(table_id_t, buf+tot_offset, table_id_);
  tot_offset += sizeof(table_id_t);
  MACH_WRITE_TO(size_t, buf+tot_offset, table_name_.length());
  tot_offset += sizeof(size_t);
  memcpy(buf + tot_offset, table_name_.c_str(), table_name_.length());
  tot_offset += table_name_.length();
  MACH_WRITE_TO(page_id_t, buf+tot_offset, root_page_id_);
  tot_offset += sizeof(page_id_t);
  tot_offset += schema_->SerializeTo(buf+tot_offset);
  return tot_offset;
}

uint32_t TableMetadata::GetSerializedSize() const {
  return sizeof(uint32_t)+sizeof(table_id_t)+sizeof(size_t)+table_name_.length()+sizeof(page_id_t)+schema_->GetSerializedSize();
}

/**
 * @param heap Memory heap passed by TableInfo
 */
uint32_t TableMetadata::DeserializeFrom(char *buf, TableMetadata *&table_meta, MemHeap *heap) {
  table_id_t table_id;
  std::string table_name;
  page_id_t root_page_id;
  TableSchema *schema;
  size_t str_len;
  uint32_t tot_offset = 0;
  uint32_t magic_num = MACH_READ_FROM(uint32_t, buf+tot_offset);
  if(magic_num == TABLE_METADATA_MAGIC_NUM){
    tot_offset += sizeof(uint32_t);
    table_id = MACH_READ_FROM(uint32_t, buf+tot_offset);
    tot_offset += sizeof(uint32_t);
    str_len = MACH_READ_FROM(size_t,buf+tot_offset);
    tot_offset += sizeof(size_t);

    char *name1 = new char[str_len];
    memcpy(name1, buf+tot_offset, str_len);
    table_name=name1;
    tot_offset += str_len;
    delete name1;

    root_page_id = MACH_READ_FROM(page_id_t,buf+tot_offset);
    tot_offset += sizeof(page_id_t);

    tot_offset += Schema::DeserializeFrom(buf+tot_offset,schema,heap);
    table_meta = ALLOC_P(heap,TableMetadata)(table_id,table_name,root_page_id,schema);
    return tot_offset;
  }else
  return 0;
}

/**
 * Only called by create table
 *
 * @param heap Memory heap passed by TableInfo
 */
TableMetadata *TableMetadata::Create(table_id_t table_id, std::string table_name,
                                     page_id_t root_page_id, TableSchema *schema, MemHeap *heap) {
  // allocate space for table metadata
  void *buf = heap->Allocate(sizeof(TableMetadata));
  return new(buf)TableMetadata(table_id, table_name, root_page_id, schema);
}

TableMetadata::TableMetadata(table_id_t table_id, std::string table_name, page_id_t root_page_id, TableSchema *schema)
        : table_id_(table_id), table_name_(table_name), root_page_id_(root_page_id), schema_(schema) {}
