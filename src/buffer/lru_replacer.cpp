#include "buffer/lru_replacer.h"

LRUReplacer::LRUReplacer(size_t num_pages) {
  max_supported_list_size = num_pages;
}

LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::Victim(frame_id_t *frame_id) {
  if( !lru_list.empty() ){
    *frame_id = lru_list.back();
    lru_list.pop_back();
    return true;
  }
  return false;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  vector<int>::iterator it = find(lru_list.begin(),lru_list.end(),frame_id);
  if( it != lru_list.end() ) lru_list.erase(it);
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  vector<int>::iterator it = find(lru_list.begin(),lru_list.end(),frame_id);
  if( it == lru_list.end() )
  {  //not in list
    if( Size() == max_supported_list_size ) //if the list is full
      lru_list.pop_back();
    lru_list.insert(lru_list.begin(),frame_id);
  }
}

size_t LRUReplacer::Size() {
  return (size_t)lru_list.size();
}