#include "catalog/catalog.h"
#include<iostream>

void CatalogMeta::SerializeTo(char *buf) const {
  uint32_t tot_offset = 0;
  MACH_WRITE_TO(uint32_t,buf+tot_offset,CATALOG_METADATA_MAGIC_NUM);
  tot_offset += sizeof(uint32_t);

  MACH_WRITE_TO(size_t,buf+tot_offset,table_meta_pages_.size());
  tot_offset += sizeof(size_t);
  for(auto it=table_meta_pages_.begin();it != table_meta_pages_.end(); it++){
    MACH_WRITE_TO(table_id_t,buf+tot_offset,it->first);
    tot_offset += sizeof(table_id_t);
    MACH_WRITE_TO(page_id_t,buf+tot_offset,it->second);
    tot_offset += sizeof(page_id_t);
  }
  
  MACH_WRITE_TO(size_t,buf+tot_offset,index_meta_pages_.size());
  tot_offset += sizeof(size_t);
  for(auto it=index_meta_pages_.begin();it != index_meta_pages_.end(); it++){
    MACH_WRITE_TO(index_id_t,buf+tot_offset,it->first);
    tot_offset += sizeof(index_id_t);
    MACH_WRITE_TO(page_id_t,buf+tot_offset,it->second);
    tot_offset += sizeof(page_id_t);
  }
}

CatalogMeta *CatalogMeta::DeserializeFrom(char *buf, MemHeap *heap) {
  uint32_t tot_offset = 0;
  uint32_t magic_num = MACH_READ_FROM(uint32_t, buf+tot_offset);

  if(magic_num != CATALOG_METADATA_MAGIC_NUM) return nullptr;
  tot_offset += sizeof(uint32_t);
  CatalogMeta *result = ALLOC_P(heap,CatalogMeta);
  table_id_t table_id_tmp;
  page_id_t page_id_tmp;
  index_id_t index_id_tmp;
  size_t size;
  
  size = MACH_READ_FROM(size_t,buf+tot_offset);
  tot_offset += sizeof(size_t);
  for(size_t i=0 ; i<size; i++){
    table_id_tmp = MACH_READ_FROM(table_id_t,buf+tot_offset);
    tot_offset += sizeof(table_id_t);
    page_id_tmp = MACH_READ_FROM(page_id_t,buf+tot_offset);
    tot_offset += sizeof(page_id_t);
    result->table_meta_pages_.insert(pair<table_id_t, page_id_t>(table_id_tmp,page_id_tmp));
  }

  size = MACH_READ_FROM(size_t,buf+tot_offset);
  tot_offset += sizeof(size_t);
  for(size_t i=0 ; i<size; i++){
    index_id_tmp = MACH_READ_FROM(index_id_t,buf+tot_offset);
    tot_offset += sizeof(index_id_t);
    page_id_tmp = MACH_READ_FROM(page_id_t,buf+tot_offset);
    tot_offset += sizeof(page_id_t);
    result->index_meta_pages_.insert(pair<index_id_t, page_id_t>(index_id_tmp,page_id_tmp));
  }

  return result;
}

uint32_t CatalogMeta::GetSerializedSize() const {
  return sizeof(uint32_t) + 2*sizeof(size_t) + 
         table_meta_pages_.size()*(sizeof(table_id_t)+sizeof(page_id_t)) + 
         index_meta_pages_.size()*(sizeof(index_id_t)+sizeof(page_id_t));
}

CatalogMeta::CatalogMeta() {}


CatalogManager::CatalogManager(BufferPoolManager *buffer_pool_manager, LockManager *lock_manager,
                               LogManager *log_manager, bool init)
        : buffer_pool_manager_(buffer_pool_manager), lock_manager_(lock_manager),
          log_manager_(log_manager), heap_(new SimpleMemHeap()) {
  if(init){
    catalog_meta_ = CatalogMeta::NewInstance(heap_);
    next_index_id_ = 0;
    next_table_id_ = 0;
    Page *meta_page;
    meta_page = buffer_pool_manager->FetchPage(CATALOG_META_PAGE_ID);
    catalog_meta_->SerializeTo(meta_page->GetData());
    buffer_pool_manager->UnpinPage(CATALOG_META_PAGE_ID,true);
  }
  else{
    Page *meta_page;
    table_id_t table_max_id = 0;
    index_id_t index_max_id = 0;
    meta_page = buffer_pool_manager->FetchPage(CATALOG_META_PAGE_ID);
    catalog_meta_ = CatalogMeta::DeserializeFrom(meta_page->GetData(),heap_);
    buffer_pool_manager->UnpinPage(CATALOG_META_PAGE_ID,false);
    for(auto it=catalog_meta_->table_meta_pages_.begin();it != catalog_meta_->table_meta_pages_.end();it++){
      LoadTable(it->first,it->second);
      table_max_id = it->first > table_max_id ? it->first:table_max_id; //记录最大id
    }
    next_table_id_ = table_max_id + 1;

    for(auto it=catalog_meta_->index_meta_pages_.begin();it != catalog_meta_->index_meta_pages_.end();it++){
      LoadIndex(it->first,it->second);
      index_max_id = it->first > table_max_id ? it->first:table_max_id; //记录最大id
    }
    next_index_id_ = index_max_id + 1;
  }
}

CatalogManager::~CatalogManager() {
  delete heap_;
  FlushCatalogMetaPage();
}

dberr_t CatalogManager::CreateTable(const string &table_name, TableSchema *schema,
                                    Transaction *txn, TableInfo *&table_info) {
  //先搞定table_heap，再搞定table_meta，注意new了一个页给meta
  TableInfo *info;
  if(GetTable(table_name,info) == DB_SUCCESS) return DB_TABLE_ALREADY_EXIST;
  table_info = TableInfo::Create(heap_);
  page_id_t meta_page_id;
  Page *meta_page;
  TableHeap *table_heap = TableHeap::Create(buffer_pool_manager_,schema,txn,log_manager_,lock_manager_,heap_);
  

  meta_page = buffer_pool_manager_->NewPage(meta_page_id);
  TableMetadata *table_meta = TableMetadata::Create(next_table_id_,table_name,table_heap->GetFirstPageId(),schema,heap_);
  
  table_meta->SerializeTo(meta_page->GetData());

  catalog_meta_->table_meta_pages_.insert(std::make_pair((table_id_t)next_table_id_,meta_page_id));

  table_info->Init(table_meta,table_heap);
  table_names_.insert(std::make_pair(table_name,(table_id_t)next_table_id_));
  tables_.insert(std::make_pair((table_id_t)next_table_id_,table_info));
  next_table_id_++;
  return DB_SUCCESS;
}

dberr_t CatalogManager::GetTable(const string &table_name, TableInfo *&table_info) {
  if(table_names_.find(table_name) == table_names_.end()) return DB_TABLE_NOT_EXIST;
  table_info = tables_.find(table_names_.find(table_name)->second)->second;
  return DB_SUCCESS;
}

dberr_t CatalogManager::GetTables(vector<TableInfo *> &tables) const {
  tables.clear();
  for(auto it=tables_.begin(); it!=tables_.end() ;it++){
    tables.push_back(it->second);
  }
  return DB_SUCCESS;
}

dberr_t CatalogManager::CreateIndex(const std::string &table_name, const string &index_name,
                                    const std::vector<std::string> &index_keys, Transaction *txn,
                                    IndexInfo *&index_info) {
  //先搞定tableinfo,再搞定indexmetadata，注意new了一个页给metadata
  if(table_names_.find(table_name) == table_names_.end()) return DB_TABLE_NOT_EXIST;
  IndexInfo *info;
  if(GetIndex(table_name,index_name,info) == DB_SUCCESS) return DB_INDEX_ALREADY_EXIST;
  index_info = IndexInfo::Create(heap_);
  page_id_t meta_page_id;
  Page *meta_page;
  std::vector<uint32_t> key_map;
  TableInfo *table_info;
  GetTable(table_name,table_info);
  for(size_t i=0;i<index_keys.size();i++){
    uint32_t index_tmp;
    if(table_info->GetSchema()->GetColumnIndex(index_keys[i],index_tmp) != DB_SUCCESS)
      return DB_COLUMN_NAME_NOT_EXIST;
    key_map.push_back(index_tmp);
  }
  if(index_names_.find(table_name)==index_names_.end()){ //假设该表未存在索引
    std::unordered_map<std::string, index_id_t> tmp;
    tmp.insert(std::make_pair(index_name,(index_id_t)next_index_id_));
    index_names_.insert(std::make_pair(table_name,tmp));
  }else{//否则就已经存在该表的索引了，在其基础上再加一个
    index_names_.find(table_name)->second.insert(std::make_pair(index_name,(index_id_t)next_index_id_));
  }
  indexes_.insert(std::make_pair((index_id_t)next_index_id_,index_info));

  meta_page = buffer_pool_manager_->NewPage(meta_page_id);
  IndexMetadata *index_metadata = IndexMetadata::Create(next_index_id_,index_name,table_names_.find(table_name)->second,key_map,heap_);
  index_metadata->SerializeTo(meta_page->GetData());

  catalog_meta_->index_meta_pages_.insert(std::make_pair((index_id_t)next_index_id_,meta_page_id));
  index_info->Init(index_metadata,table_info,buffer_pool_manager_);
  next_index_id_++;

  [[maybe_unused]] Index *index = index_info->GetIndex();
  [[maybe_unused]] Schema *schema = table_info->GetSchema();
  // auto keyt = table_info->GetTableHeap()->Begin(txn);
  // auto keyt2 = table_info->GetTableHeap()->End();
  [[maybe_unused]] dberr_t status;
  for (auto tuple = table_info->GetTableHeap()->Begin(txn); tuple != table_info->GetTableHeap()->End(); tuple++) {
    // LOG(INFO) << 1;
    IndexSchema *indSchema = index_info->GetIndexKeySchema();
    std::vector<int> indexColumns;
    indexColumns.clear();
    for (int i = 0; i < (int)indSchema->GetColumnCount(); ++i) {
      uint32_t ind = 0;
      if((status = schema->GetColumnIndex(indSchema->GetColumn(i)->GetName(), ind)) != DB_SUCCESS) {
        return status;
      }
      indexColumns.push_back(ind);
    }
    std::vector<Field> key;
    key.clear();
    for (int i = 0; i < (int)indSchema->GetColumnCount(); ++i) {
      key.push_back(*(tuple->GetField(indexColumns[i])));
    }
    Row keyRow(key);
    //
    RowId rid = tuple->GetRowId();
    // std::vector<Field> row;
    // for (int i = 0; i < (int)key_map.size(); ++i) {
    //   row.push_back(*(key->GetField(key_map[i])));
    // }
    // Row keyRow(row);
    keyRow.SetRowId(rid);
    // LOG(INFO) << "rid: " << rid.GetPageId();
    if (index->InsertEntry(keyRow, rid, txn) != DB_SUCCESS) {
      // LOG(ERROR) << "InsertEntry failed";
      std::cerr << "Duplicate key" << std::endl;
      DropIndex(table_name, index_name);
      return DB_FAILED;
    }
  }

  return DB_SUCCESS;
}

dberr_t CatalogManager::GetIndex(const std::string &table_name, const std::string &index_name,
                                 IndexInfo *&index_info) const {
  index_info = nullptr;
  auto tmp1 = index_names_.find(table_name);
  if(tmp1 == index_names_.end()) return DB_INDEX_NOT_FOUND;
  auto tmp2 = tmp1->second.find(index_name);
  if(tmp2 == tmp1->second.end()) return DB_INDEX_NOT_FOUND;
  index_info = indexes_.find(tmp2->second)->second;
  return DB_SUCCESS;
}

dberr_t CatalogManager::GetTableIndexes(const std::string &table_name, std::vector<IndexInfo *> &indexes) const {
  if(table_names_.find(table_name) == table_names_.end()) return DB_TABLE_NOT_EXIST;
  auto it = index_names_.find(table_name);
  if(it == index_names_.end()) return DB_SUCCESS;
  for(auto ite = it->second.begin();ite != it->second.end();ite++){
    indexes.push_back(indexes_.find(ite->second)->second);
  }
  return DB_SUCCESS;
}

dberr_t CatalogManager::DropTable(const string &table_name) {
  auto it = table_names_.find(table_name);
  if(it == table_names_.end()) return DB_TABLE_NOT_EXIST;
  table_id_t id = it->second;
  table_names_.erase(table_name);
  tables_.erase(id);
  auto it1 = index_names_.find(table_name);
  if(it1 != index_names_.end()){
    auto it2 = it1->second;
    for(auto it3 = it2.begin();it3!=it2.end();it3++){
      indexes_.erase(it3->second);
      catalog_meta_->index_meta_pages_.erase(it3->second);
    }
    index_names_.erase(table_name);
  }
  catalog_meta_->table_meta_pages_.erase(id);
  return DB_SUCCESS;
}

dberr_t CatalogManager::DropIndex(const string &table_name, const string &index_name) {
  auto it = index_names_.find(table_name);
  if(it == index_names_.end()) return DB_TABLE_NOT_EXIST;
  auto it2 = it->second;
  auto it3 = it2.find(index_name);
  if(it3 == it2.end()) return DB_INDEX_NOT_FOUND;
  else {
    index_id_t index_id = it3->second;
    indexes_[index_id]->GetIndex()->Destroy();
    indexes_.erase(it3->second);
    catalog_meta_->index_meta_pages_.erase(it3->second);
    it->second.erase(index_name);
  }
  return DB_SUCCESS;
}


dberr_t CatalogManager::FlushCatalogMetaPage() const {
  Page *meta_page;
  meta_page = buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID);
  catalog_meta_->SerializeTo(meta_page->GetData());
  buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID,true);
  buffer_pool_manager_->FlushPage(CATALOG_META_PAGE_ID);
  return DB_SUCCESS;
}

dberr_t CatalogManager::LoadTable(const table_id_t table_id, const page_id_t page_id) {
  Page* meta_page = buffer_pool_manager_->FetchPage(page_id);
  TableMetadata *meta_data;

  TableMetadata::DeserializeFrom(meta_page->GetData(),meta_data,heap_);

  TableHeap *table_heap = TableHeap::Create(buffer_pool_manager_,meta_data->GetFirstPageId(),meta_data->GetSchema(),log_manager_,lock_manager_,heap_);
  TableInfo *info = TableInfo::Create(heap_);
  info->Init(meta_data,table_heap);
  table_names_.insert(std::make_pair(meta_data->GetTableName(),meta_data->GetTableId()));
  tables_.insert(std::make_pair(meta_data->GetTableId(),info));
  return DB_SUCCESS;
}

dberr_t CatalogManager::LoadIndex(const index_id_t index_id, const page_id_t page_id) {
  Page* meta_page = buffer_pool_manager_->FetchPage(page_id);
  IndexMetadata *meta_data;
  IndexMetadata::DeserializeFrom(meta_page->GetData(),meta_data,heap_);

  std::vector<uint32_t> key_map;
  TableInfo *table_info;
  GetTable(meta_data->GetTableId(),table_info);
  
  auto it = index_names_.find(table_info->GetTableName());
  if(it == index_names_.end()){
    std::unordered_map<std::string, index_id_t> tmp;
    tmp.insert(std::make_pair(meta_data->GetIndexName(),meta_data->GetIndexId()));
    index_names_.insert(std::make_pair(table_info->GetTableName(),tmp));
  }else{
    it->second.insert(std::make_pair(meta_data->GetIndexName(),meta_data->GetIndexId()));
  }
  IndexInfo *info = IndexInfo::Create(heap_);
  info->Init(meta_data,table_info,buffer_pool_manager_);
  indexes_.insert(std::make_pair(meta_data->GetIndexId(),info));
  return DB_SUCCESS;
}

dberr_t CatalogManager::GetTable(const table_id_t table_id, TableInfo *&table_info) {
  table_info = tables_.find(table_id)->second;
  return DB_SUCCESS;
}