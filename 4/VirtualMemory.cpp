/**
 * @author natashashuklin cs
 * @brief ex4 - OS
 */
#include "VirtualMemory.h"
#include "PhysicalMemory.h"

/*
 * clear
 */
void clearTable(uint64_t frameIndex) {
  for (uint64_t i = 0; i < PAGE_SIZE; ++i) {
    PMwrite(frameIndex * PAGE_SIZE + i, 0);
  }
}
/**
 * @brief init
 */
void VMinitialize() {
  clearTable(0);
}
/**
 * @brief get the weight of page or frame corresponding to its index
 * @param frame
 * @return
 */
int get_weight_page_or_frame(int frame) {
  if (frame % 2 != 0) {
    return WEIGHT_ODD;
  }
  return WEIGHT_EVEN;
}
/**
 * @brief get max weight path for dfs tree traverse
 * @param page_index_to_evict
 * @param max_weight
 * @param cur_weight
 * @param page_index
 * @param current_page
 * @param max_page
 * @param current_frame
 * @param max_frame
 * @param max_parent_f
 * @param current_parent
 */
void getMaxWeightPath(uint64_t &page_index_to_evict, int & max_weight, int &cur_weight, uint64_t page_index,
                      uint64_t &current_page,  uint64_t current_parent,
                      uint64_t &max_page, word_t current_frame,
                      word_t &max_frame, uint64_t &max_parent_f) {
  if ((cur_weight > max_weight) || ((cur_weight == max_weight) && (page_index < page_index_to_evict))) {
    max_page = current_page;
    max_frame = current_frame;
    page_index_to_evict = current_page;
    max_parent_f = current_parent;
  }
}
/**
 * @brief traverse tree and find frame, and if needed also find the max path
 * for enviction
 * weight for cond.3
 * @param page_index_to_evict
 * @param current_weight
 * @param max_weight
 * @param current_depth
 * @param current_frame
 * @param free_f
 * @param current_parent
 * @param free_parent_f
 * @param curent_parent_add
 * @param prev_parent_add
 * @param max_parent_add
 * @param isempty
 * @param max_frame_i
 * @param page_restore
 * @param max_page
 * @param max_frame
 * @param max_parent_f
 * @param page_index
 * @param next_parent_f
 */
void traverseTreeDFS(uint64_t &page_index_to_evict, int & current_weight, int &max_weight, int current_depth, word_t current_frame,
                     word_t& free_f, uint64_t page_index,
                     word_t current_parent, word_t& free_parent_f,
                     uint64_t curent_parent_add, uint64_t& prev_parent_add,
                     uint64_t& max_parent_add,
                     int &max_frame_i, bool &isempty,uint64_t page_restore,
                     uint64_t& max_page, word_t &max_frame,
                     uint64_t &max_parent_f,
                     word_t next_parent_f) {
  //get weight
  current_weight += get_weight_page_or_frame(current_frame);
  int amount_empty = 0;
  if (current_depth == TABLES_DEPTH) {
    current_weight += get_weight_page_or_frame(page_index);
    getMaxWeightPath(page_index_to_evict,
                     max_weight,
                     current_weight,
                     page_index,
                     page_restore,
                     curent_parent_add,
                     max_page,
                     current_frame,
                     max_frame,
                     max_parent_add);
    return;
  }
  if (current_frame != 0) {
    current_parent = current_frame;
  }
  uint64_t current_pr_parent = 0;
  for (int i = 0; i < PAGE_SIZE; i++) {
    word_t temp_val;
    if (amount_empty == 0) {
      //update parent
      current_pr_parent = curent_parent_add;
    }
    curent_parent_add = (uint64_t) current_parent * PAGE_SIZE;
    curent_parent_add = curent_parent_add + i;
    PMread(curent_parent_add, &temp_val);
    if (temp_val != 0) {
      current_frame = temp_val;
      if (current_frame > max_frame_i) {
        max_frame_i = current_frame;
      }
      page_restore = (page_restore << OFFSET_WIDTH) + i;
      traverseTreeDFS(page_index_to_evict,
                      current_weight,
                      max_weight,
                      current_depth + 1,
                      current_frame,
                      free_f,
                      page_index,
                      current_parent,
                      free_parent_f,
                      curent_parent_add,
                      prev_parent_add,
                      max_parent_add,
                      max_frame_i,
                      isempty,
                      page_restore,
                      max_page,
                      max_frame,
                      max_parent_f,
                      next_parent_f);
      page_restore = page_restore >> OFFSET_WIDTH;
    } else {
      amount_empty = amount_empty + 1;
    }
    if (amount_empty == PAGE_SIZE && next_parent_f != current_frame) {
      //update avail.
      free_f = current_frame;
      free_parent_f = current_parent;
      isempty = true;
      //save prev
      prev_parent_add = current_pr_parent;
      return;
    }
  }
}
/**checks clear table
 * @brief
 * @param frameIndex
 * @return
 */
bool checkEmptyTable(uint64_t frameIndex)
{ word_t value;
  for (uint64_t i = 0; i < PAGE_SIZE; ++i)
  {
    PMread(frameIndex * PAGE_SIZE + i, &value);
    if (value != 0) {
      return false;
    }
  }
  return true;
}
/*helper func to vmread write
 */
void helperFunc(uint64_t &addr, word_t &page_index, bool isNeeded, unsigned
int k, uint64_t virtualAddress, unsigned int beg, unsigned int fin){
  unsigned int first=0, last=0;
  uint64_t b = 0;
  unsigned int amount_o = VIRTUAL_ADDRESS_WIDTH;
  amount_o = amount_o - OFFSET_WIDTH;
  unsigned int amount_s = amount_o%OFFSET_WIDTH;
  beg = first;
  fin = last;
  unsigned int beg_init = VIRTUAL_ADDRESS_WIDTH-1;
  if(!isNeeded){
    for (unsigned int i=(unsigned int)OFFSET_WIDTH; i<=(unsigned int)
        (VIRTUAL_ADDRESS_WIDTH - 1); i++) {
      b |= 1 << i;
    }
    b = b&virtualAddress;
    for (unsigned int j=0; j < (unsigned int)OFFSET_WIDTH; j++){
      b = b >> 1;
    }
    page_index = b;
  }
  else{
    if (k != 0){
      if (amount_s == 0){
        beg = first;
        first = beg_init - (k * OFFSET_WIDTH);
      }else{
        first = beg_init;
        beg = first;
        first = first - amount_s;
        first = first- ((k-1) * OFFSET_WIDTH);
      }
      last = first - OFFSET_WIDTH + 1;
      beg = first;
    }
    else{
      first = beg_init;
      if (amount_s == 0){
        fin = last;
        last = first - OFFSET_WIDTH + 1;
      }
      else{
        last = first - amount_s + 1;
        fin = last;
      }
    }
    for (unsigned int i=last; i<=first; i++) {
      b |= 1 << i;
    }
    b = b&virtualAddress;
    beg = first;
    for (unsigned int j=0; j < last; j++){
      b = b >> 1;
    }
    addr=b;
  }
}
/* vm read write help fun
 */
word_t VMreadWriteAssist(uint64_t virtualAddress, bool write, word_t *value){
  word_t page_index = 0, current_frame=0,available_table = 0, parent_available_t = 0;
  uint64_t page_index_to_evict =0,addr = 0,max_page = 0,max_parent_add =0,
      current_parent = 0;
  uint64_t prev_parent_add=0, page_restore = 0;
  word_t val=0, prev_parent_f=0, max_frame_p=0;
  word_t next_parent_f=0;
  int max_index_frame;
  int max_weight = 0;
  uint64_t next_parent_add;
  bool is_empty;
  int weight =0;
  bool is_need_bit=false;
  helperFunc(addr, page_index, is_need_bit, 0, virtualAddress, 0, 0);
  for(int i=0; i< TABLES_DEPTH; ++i){
    is_need_bit=true;
    helperFunc(addr, page_index, is_need_bit, (unsigned int)i, virtualAddress,
               0, 0);
    next_parent_add = next_parent_f*PAGE_SIZE;
    next_parent_add+= addr;
    PMread(next_parent_add, &val);
    if(val !=0){
      next_parent_f = val;
    }
    else{
      prev_parent_add = 0;
      parent_available_t = 0;
      max_page = page_index;
      is_empty = false;
      current_frame=0;
      current_parent = 0;
      max_index_frame = 0;
      prev_parent_f =0;
      page_restore = 0;
      //traverse tree
      traverseTreeDFS(page_index_to_evict,
                      weight,
                      max_weight,
                      0,
                      current_frame,
                      available_table,
                      page_index,
                      prev_parent_f,
                      parent_available_t,
                      current_parent,
                      prev_parent_add,
                      max_parent_add,
                      max_index_frame,
                      is_empty,
                      page_restore,
                      max_page,
                      max_frame_p,
                      max_parent_add,
                      next_parent_f);

      if (next_parent_f != available_table && checkEmptyTable(available_table)){
        PMwrite(prev_parent_add, 0);
      }
      else if (max_index_frame + 1<NUM_FRAMES)
      {
        available_table = max_index_frame + 1;
      }
      else
      { //condition 3
        PMwrite(max_parent_add, 0);
        PMevict((uint64_t )max_frame_p, max_page);
        available_table = max_frame_p;
      }
      if (i == TABLES_DEPTH -1){
        PMrestore((uint64_t )available_table, page_index);
      }else{
        clearTable((uint64_t )available_table);
      }
      PMwrite(next_parent_add, available_table);
      next_parent_f = available_table;
    }
  }
  if(write){
    uint64_t offset = virtualAddress & ((1LL << OFFSET_WIDTH) - 1);
    PMwrite((long long) next_parent_f * PAGE_SIZE + offset, *value);
  }
  else{
    uint64_t offset = virtualAddress & ((1LL << OFFSET_WIDTH) - 1);
    PMread((long long) next_parent_f * PAGE_SIZE + offset, value);
  }
  return next_parent_f;
}
/**
 * @brief vm read
 * @param virtualAddress
 * @param value
 * @return
 */
int VMread(uint64_t virtualAddress, word_t* value) {
//  if (virtualAddress >= VIRTUAL_MEMORY_SIZE){
//    return 0;
//  }
    if (virtualAddress >= VIRTUAL_MEMORY_SIZE ||
    TABLES_DEPTH + 1 > NUM_FRAMES||
    VIRTUAL_ADDRESS_WIDTH ==0 || PHYSICAL_ADDRESS_WIDTH ==0||RAM_SIZE <= TABLES_DEPTH)
    {
        return 0;
    }

    VMreadWriteAssist(virtualAddress, false,value);

    return 1;
}

/**
 * @brief wm write
 * @param virtualAddress
 * @param value
 * @return
 */
int VMwrite(uint64_t virtualAddress, word_t value) {
    if (virtualAddress >= VIRTUAL_MEMORY_SIZE ||
        TABLES_DEPTH + 1 > NUM_FRAMES||
        VIRTUAL_ADDRESS_WIDTH ==0 || PHYSICAL_ADDRESS_WIDTH ==0||RAM_SIZE <= TABLES_DEPTH)
    {
        return 0;
    }
    VMreadWriteAssist(virtualAddress, true, &value);
    return 1;

}