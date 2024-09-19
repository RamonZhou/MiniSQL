#include "page/bitmap_page.h"

template<size_t PageSize>
bool BitmapPage<PageSize>::AllocatePage(uint32_t &page_offset) {
  if(page_allocated_ < 8 * MAX_CHARS){ //if have space
    for( next_free_page_ = 0 ; next_free_page_ < 8 * MAX_CHARS ;next_free_page_++ ){ 
      if( IsPageFree(next_free_page_) ) break;
    }
    uint32_t byte_index = next_free_page_ / 8;
    uint8_t bit_index = next_free_page_ % 8;
    uint8_t num;
    num = 1 << bit_index;
    bytes[byte_index] += num; //set the bytes array
    page_offset = next_free_page_;
    page_allocated_++;
    /*
     * search begin with next_free_page_
     * because if a smaller index page is deleted
     * next_free_page would change
     */
    return true;
  } else return false;
}

template<size_t PageSize>
bool BitmapPage<PageSize>::DeAllocatePage(uint32_t page_offset) {
  if( page_offset >= 8 * MAX_CHARS || page_offset < 0 ) return false;
  if( IsPageFree(page_offset) ) return false;
  else{
    uint32_t byte_index = page_offset / 8;
    uint8_t bit_index = page_offset % 8;
    uint8_t num;
    num = 1 << bit_index;
    bytes[byte_index] -= num; //set the bytes array
    page_allocated_--;
    return true;
  }
  return true;
}

template<size_t PageSize>
bool BitmapPage<PageSize>::IsPageFree(uint32_t page_offset) const {
  return IsPageFreeLow( page_offset / 8 , page_offset % 8 ); //check low
}

template<size_t PageSize>
bool BitmapPage<PageSize>::IsPageFreeLow(uint32_t byte_index, uint8_t bit_index) const {
  uint8_t num = 1 << bit_index; //use num to check used
  if( (bytes[byte_index] & num) == 0) return true; //AND check 0
  else return false;
  return false;
}

template
class BitmapPage<64>;

template
class BitmapPage<128>;

template
class BitmapPage<256>;

template
class BitmapPage<512>;

template
class BitmapPage<1024>;

template
class BitmapPage<2048>;

template
class BitmapPage<4096>;