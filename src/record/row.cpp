#include "record/row.h"

uint32_t Row::SerializeTo(char *buf, Schema *schema) const {
  // replace with your code here
  uint32_t tot_offset = 0;
  MACH_WRITE_TO(uint32_t, buf + tot_offset, this->ROW_MAGIC_NUM);
  tot_offset += sizeof(uint32_t);
  MACH_WRITE_TO(size_t, buf + tot_offset, this->fields_.size());
  tot_offset += sizeof(size_t);
  MACH_WRITE_TO(RowId, buf + tot_offset, this->rid_);
  tot_offset += sizeof(RowId);

  char *bitmap = new char[this->fields_.size()/8+1];
  memset(bitmap,0,this->fields_.size()/8+1);
  for(size_t i = 0;i < this->fields_.size();i++){
    uint32_t byte_index = i / 8;
    uint32_t bit_index = i % 8;
    uint8_t num = 1 << bit_index;
    if(!this->fields_[i]->IsNull()){
      bitmap[byte_index] += num;
    }
  }
  for(size_t i = 0;i < this->fields_.size()/8+1; i++){
    MACH_WRITE_TO(char, buf + tot_offset + i, bitmap[i]);
  }
  tot_offset += fields_.size()/8+1;

  for(size_t i = 0;i < this->fields_.size();i++){
    tot_offset += this->fields_[i]->SerializeTo(buf + tot_offset);
  }
  delete bitmap;
  return tot_offset;
}

uint32_t Row::DeserializeFrom(char *buf, Schema *schema) {
  // replace with your code here
  uint32_t tot_offset = 0;
  uint32_t magic_num = MACH_READ_FROM(uint32_t, buf);
  if(magic_num == ROW_MAGIC_NUM){
    tot_offset += sizeof(uint32_t);
    size_t size_of_fields = MACH_READ_FROM(size_t, buf + tot_offset);
    tot_offset += sizeof(size_t);
    RowId row_id = MACH_READ_FROM(RowId, buf + tot_offset);
    tot_offset += sizeof(RowId);

    size_t size_of_bitmap = size_of_fields/8+1;
    char *bitmap = new char[size_of_bitmap];
    memset(bitmap,0,size_of_bitmap);
    for(size_t i = 0; i < size_of_bitmap; i++){
      bitmap[i] = MACH_READ_FROM(char, buf + tot_offset + i);
    }
    tot_offset += size_of_bitmap;

    Field *tmp;
    this->fields_.clear();
    for(size_t i = 0; i < size_of_fields; i++){
      uint32_t byte_index = i / 8;
      uint32_t bit_index = i % 8;
      uint8_t num = 1 << bit_index;
      if((bitmap[byte_index] & num) == 0){ //is null
        tot_offset += Field::DeserializeFrom(buf + tot_offset,schema->GetColumn(i)->GetType(),
                                  &tmp,true,this->heap_);
      }else{//not null
        tot_offset += Field::DeserializeFrom(buf + tot_offset,schema->GetColumn(i)->GetType(),
                                  &tmp,false,this->heap_);
      }
      this->fields_.push_back(tmp);
    }
    this->rid_ = row_id;
    delete bitmap;
    return tot_offset;
  }else
  return 0;
}

uint32_t Row::GetSerializedSize(Schema *schema) const {
  // replace with your code here
  uint32_t tot_offset = 0;
  tot_offset += sizeof(uint32_t) + sizeof(size_t) + sizeof(RowId);
  tot_offset += this->fields_.size()/8+1;
  for(size_t i = 0;i < this->fields_.size();i++){
    tot_offset += this->fields_[i]->GetSerializedSize();
  }
  return tot_offset;
}
