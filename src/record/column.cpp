#include "record/column.h"

Column::Column(std::string column_name, TypeId type, uint32_t index, bool nullable, bool unique)
        : name_(std::move(column_name)), type_(type), table_ind_(index),
          nullable_(nullable), unique_(unique) {
  ASSERT(type != TypeId::kTypeChar, "Wrong constructor for CHAR type.");
  switch (type) {
    case TypeId::kTypeInt :
      len_ = sizeof(int32_t);
      break;
    case TypeId::kTypeFloat :
      len_ = sizeof(float_t);
      break;
    default:
      ASSERT(false, "Unsupported column type.");
  }
}

Column::Column(std::string column_name, TypeId type, uint32_t length, uint32_t index, bool nullable, bool unique)
        : name_(std::move(column_name)), type_(type), len_(length),
          table_ind_(index), nullable_(nullable), unique_(unique) {
  ASSERT(type == TypeId::kTypeChar, "Wrong constructor for non-VARCHAR type.");
}

Column::Column(const Column *other) : name_(other->name_), type_(other->type_), len_(other->len_),
                                      table_ind_(other->table_ind_), nullable_(other->nullable_),
                                      unique_(other->unique_) {}

uint32_t Column::SerializeTo(char *buf) const {
  // replace with your code here
  uint32_t tot_offset = 0;
  MACH_WRITE_TO(uint32_t, buf+tot_offset, this->COLUMN_MAGIC_NUM);
  tot_offset += sizeof(uint32_t);
  MACH_WRITE_TO(uint32_t, buf+tot_offset, this->len_);
  tot_offset += sizeof(uint32_t);
  MACH_WRITE_TO(uint32_t, buf+tot_offset, this->table_ind_);
  tot_offset += sizeof(uint32_t);
  MACH_WRITE_TO(TypeId, buf+tot_offset, this->type_);
  tot_offset += sizeof(TypeId);
  MACH_WRITE_TO(bool, buf+tot_offset, this->nullable_);
  tot_offset += sizeof(bool);
  MACH_WRITE_TO(bool, buf+tot_offset, this->unique_);
  tot_offset += sizeof(bool);
  MACH_WRITE_TO(size_t, buf+tot_offset, this->name_.length());
  tot_offset += sizeof(size_t);
  memcpy(buf + tot_offset, this->name_.c_str(), this->name_.length());
  tot_offset += this->name_.length();
  return tot_offset;
}

uint32_t Column::GetSerializedSize() const {
  // replace with your code here
  return 3*sizeof(uint32_t) + sizeof( TypeId ) + 2*sizeof(bool) + sizeof(size_t) + this->name_.length();
}

uint32_t Column::DeserializeFrom(char *buf, Column *&column, MemHeap *heap) {
  // replace with your code here
  uint32_t tot_offset = 0;
  uint32_t magic_num = MACH_READ_FROM(uint32_t, buf+tot_offset);
  if(magic_num == COLUMN_MAGIC_NUM){
    tot_offset += sizeof(uint32_t);
    uint32_t len = MACH_READ_FROM(uint32_t, buf + tot_offset);
    tot_offset += sizeof(uint32_t);
    uint32_t table_ind = MACH_READ_FROM(uint32_t, buf + tot_offset);
    tot_offset += sizeof(uint32_t);
    TypeId type = MACH_READ_FROM(TypeId, buf + tot_offset);
    tot_offset += sizeof(TypeId);
    bool nullable = MACH_READ_FROM(bool, buf + tot_offset);
    tot_offset += sizeof(bool);
    bool unique = MACH_READ_FROM(bool, buf + tot_offset);
    tot_offset += sizeof(bool);
    size_t str_len = MACH_READ_FROM(size_t, buf + tot_offset);
    tot_offset += sizeof(size_t);
    
    std::string name;
    char *name1 = new char[str_len];
    memcpy(name1, buf+tot_offset, str_len);
    name=name1;
    delete name1;
    tot_offset += str_len;
    if(type == kTypeChar)
      column = ALLOC_P(heap, Column)(name,type,len,table_ind,nullable,unique);
    else
      column = ALLOC_P(heap, Column)(name,type,table_ind,nullable,unique);
    return tot_offset;
  }else return 0;
}
