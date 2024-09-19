#include "record/schema.h"
#include <iostream>

uint32_t Schema::SerializeTo(char *buf) const {
  // replace with your code here
  uint32_t tot_offset = 0;
  MACH_WRITE_TO(uint32_t, buf+tot_offset, this->SCHEMA_MAGIC_NUM);
  tot_offset += sizeof(uint32_t);
  MACH_WRITE_TO(size_t, buf+tot_offset, this->columns_.size());
  tot_offset += sizeof(size_t);
  for(size_t i=0 ; i<this->columns_.size(); i++){
    tot_offset += columns_[i]->SerializeTo(buf+tot_offset);
  }
  return tot_offset;
}

uint32_t Schema::GetSerializedSize() const {
  // replace with your code here
  uint32_t tot_offset = sizeof(uint32_t)+sizeof(size_t);
  for(size_t i=0 ; i<this->columns_.size(); i++){
    tot_offset += columns_[i]->GetSerializedSize();
  }
  return tot_offset;
}

uint32_t Schema::DeserializeFrom(char *buf, Schema *&schema, MemHeap *heap) {
  // replace with your code here
  uint32_t tot_offset = 0;
  uint32_t magic_num = MACH_READ_FROM(uint32_t, buf+tot_offset);
  if(magic_num == SCHEMA_MAGIC_NUM){
    tot_offset += sizeof(uint32_t);
    size_t num = MACH_READ_FROM(size_t, buf + tot_offset);
    tot_offset += sizeof(size_t);
    std::vector<Column *> columns;
    Column *tmp;
    for(size_t i = 0 ; i < num ; i++){
      tot_offset += Column::DeserializeFrom(buf + tot_offset, tmp ,heap);
      columns.push_back(tmp);
    }
    schema = ALLOC_P(heap, Schema)(columns);
    return tot_offset;
  }else return 0;
  return 0;
}