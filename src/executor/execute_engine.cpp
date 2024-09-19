#include "executor/execute_engine.h"
#include "glog/logging.h"

ExecuteEngine::ExecuteEngine() {
  fstream dbs_file;
  dbs_file.open("dbs.txt", ios::in);
  if (!dbs_file.is_open()) {
    dbs_file.clear();
    dbs_file.open("dbs.txt", ios::trunc | ios::out);
    dbs_file.close();
    dbs_file.open("dbs.txt", ios::in);
    if (!dbs_file.is_open()) {
      LOG(ERROR) << "Failed to open dbs.txt";
      return;
    }
  }
  dbs_.clear();
  string db_name;
  while (getline(dbs_file, db_name)) {
    dbs_.insert(make_pair(db_name, new DBStorageEngine(db_name, false, DEFAULT_BUFFER_POOL_SIZE)));
  }
  dbs_file.close();
  current_db_ = "";
}

dberr_t ExecuteEngine::Execute(pSyntaxNode ast, ExecuteContext *context) {
  if (ast == nullptr) {
    return DB_FAILED;
  }
  context->StartRunning();
  dberr_t ret = DB_FAILED;
  bool affected = true;
  switch (ast->type_) {
    case kNodeCreateDB:
      ret = ExecuteCreateDatabase(ast, context);
      affected = true;
      break;
    case kNodeDropDB:
      ret = ExecuteDropDatabase(ast, context);
      affected = true;
      break;
    case kNodeShowDB:
      ret = ExecuteShowDatabases(ast, context);
      affected = false;
      break;
    case kNodeUseDB:
      ret = ExecuteUseDatabase(ast, context);
      affected = true;
      break;
    case kNodeShowTables:
      ret = ExecuteShowTables(ast, context);
      affected = false;
      break;
    case kNodeCreateTable:
      ret = ExecuteCreateTable(ast, context);
      affected = true;
      break;
    case kNodeDropTable:
      ret = ExecuteDropTable(ast, context);
      affected = true;
      break;
    case kNodeShowIndexes:
      ret = ExecuteShowIndexes(ast, context);
      affected = false;
      break;
    case kNodeCreateIndex:
      ret = ExecuteCreateIndex(ast, context);
      affected = true;
      break;
    case kNodeDropIndex:
      ret = ExecuteDropIndex(ast, context);
      affected = true;
      break;
    case kNodeSelect:
      ret = ExecuteSelect(ast, context);
      affected = false;
      break;
    case kNodeInsert:
      ret = ExecuteInsert(ast, context);
      affected = true;
      break;
    case kNodeDelete:
      ret = ExecuteDelete(ast, context);
      affected = true;
      break;
    case kNodeUpdate:
      ret = ExecuteUpdate(ast, context);
      affected = true;
      break;
    case kNodeTrxBegin:
      ret = ExecuteTrxBegin(ast, context);
      affected = true;
      break;
    case kNodeTrxCommit:
      ret = ExecuteTrxCommit(ast, context);
      affected = true;
      break;
    case kNodeTrxRollback:
      ret = ExecuteTrxRollback(ast, context);
      affected = true;
      break;
    case kNodeExecFile:
      ret = ExecuteExecfile(ast, context);
      affected = false;
      break;
    case kNodeQuit:
      ret = ExecuteQuit(ast, context);
      affected = true;
      break;
    default:
      ret = DB_FAILED;
      affected = false;
  }
  if (ast->type_ != kNodeQuit) context->PrintResult(ret, affected);
  context->StopRunning(ast->type_ != kNodeQuit);
  return ret;
}

dberr_t ExecuteEngine::ExecuteCreateDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateDatabase" << std::endl;
#endif
  pSyntaxNode db_name_node = ast->child_;
  std::string db_name = db_name_node->val_;
  if (dbs_.find(db_name) != dbs_.end()) {
    return DB_FAILED;
  }
  fstream dbs_file;
  dbs_file.open("dbs.txt", ios::out);
  if (!dbs_file.is_open()) {
    LOG(ERROR) << "Failed to open dbs.txt";
    return DB_FAILED;
  }
  for (auto it = dbs_.begin(); it != dbs_.end(); ++it) {
    dbs_file << it->first << std::endl;
  }
  dbs_file << db_name << std::endl;
  dbs_file.close();
  dbs_.insert(make_pair(db_name, new DBStorageEngine(db_name, true, DEFAULT_BUFFER_POOL_SIZE)));
  context->AddAffectedRows();
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteDropDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropDatabase" << std::endl;
#endif
  pSyntaxNode db_name_node = ast->child_;
  std::string db_name = db_name_node->val_;
  if (dbs_.find(db_name) == dbs_.end()) {
    return DB_FAILED;
  }
  fstream dbs_file;
  dbs_file.open("dbs.txt", ios::out);
  if (!dbs_file.is_open()) {
    LOG(ERROR) << "Failed to open dbs.txt";
    return DB_FAILED;
  }
  for (auto it = dbs_.begin(); it != dbs_.end(); ++it) {
    if (it->first != db_name) dbs_file << it->first << std::endl;
  }
  dbs_file.close();
  dbs_.erase(db_name);
  remove(db_name.c_str());
  if (current_db_ == db_name) {
    current_db_ = "";
  }
  context->AddAffectedRows();
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteShowDatabases(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowDatabases" << std::endl;
#endif
  unsigned int max_len = 9;
  for (auto it = dbs_.begin(); it != dbs_.end(); ++it) {
    max_len = max(max_len, (unsigned int)it->first.length());
  }
  context->PrintDivider(max_len);
  context->PrintString("Databases", max_len);
  context->PrintDivider(max_len);
  for (auto it = dbs_.begin(); it != dbs_.end(); ++it) {
    context->PrintString(it->first, max_len);
    context->AddNumSelectedRows();
  }
  context->PrintDivider(max_len);
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteUseDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteUseDatabase" << std::endl;
#endif
  pSyntaxNode db_name_node = ast->child_;
  std::string db_name = db_name_node->val_;
  if (dbs_.find(db_name) == dbs_.end()) {
    return DB_FAILED;
  }
  current_db_ = db_name;
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteShowTables(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowTables" << std::endl;
#endif
  if (current_db_ == "") {
    return DB_FAILED;
  }
  DBStorageEngine *db = dbs_[current_db_];
  if (db == nullptr) {
    return DB_FAILED;
  }
  std::vector<TableInfo *> tables;
  dberr_t status;
  if ((status = db->catalog_mgr_->GetTables(tables)) != DB_SUCCESS) {
    return status;
  }
  unsigned int max_len = 6;
  for (int i = 0; i < (int)tables.size(); ++i) {
    max_len = max(max_len, (unsigned int)tables[i]->GetTableName().length());
  }
  context->PrintDivider(max_len);
  context->PrintString("Tables", max_len);
  context->PrintDivider(max_len);
  for (int i = 0; i < (int)tables.size(); ++i) {
    context->PrintString(tables[i]->GetTableName(), max_len);
    context->AddNumSelectedRows();
  }
  context->PrintDivider(max_len);
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteCreateTable(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateTable" << std::endl;
#endif
  if (current_db_ == "") {
    return DB_FAILED;
  }
  DBStorageEngine *db = dbs_[current_db_];
  if (db == nullptr) {
    return DB_FAILED;
  }
  pSyntaxNode table_name_node = ast->child_;
  pSyntaxNode column_list_node = table_name_node->next_;
  std::string table_name = table_name_node->val_;
  std::vector<Column *> columns;
  std::vector<std::string> primary_keys;
  uint32_t ind = 0;
  for (pSyntaxNode column_node = column_list_node->child_; column_node != nullptr; column_node = column_node->next_, ++ ind) {
    if (column_node->type_ == kNodeColumnDefinition) {
      std::string column_unique = column_node->val_ == nullptr ? "" : column_node->val_;
      bool unique = (column_unique == "unique");
      pSyntaxNode column_name_node = column_node->child_;
      pSyntaxNode column_type_node = column_name_node->next_;
      std::string column_name = column_name_node->val_;
      std::string column_type = column_type_node->val_;
      if (column_type == "int") {
        columns.push_back(new Column(column_name, TypeId::kTypeInt, ind, !unique, unique));
      } else if (column_type == "float") {
        columns.push_back(new Column(column_name, TypeId::kTypeFloat, ind, !unique, unique));
      } else if (column_type == "char") {
        pSyntaxNode column_length_node = column_type_node->child_;
        int column_length = atoi(column_length_node->val_);
        if (column_length <= 0) {
          return DB_FAILED;
        }
        columns.push_back(new Column(column_name, TypeId::kTypeChar, column_length, ind, !unique, unique));
      } else {
        for (int i = 0; i < (int)columns.size(); ++i) {
          delete columns[i];
        }
        return DB_FAILED;
      }
    } else {
      std::string column_type = column_node->val_ == nullptr ? "" : column_node->val_;
      if (column_type == "primary keys") {
        for (pSyntaxNode primary_key_node = column_node->child_; primary_key_node != nullptr; primary_key_node = primary_key_node->next_) {
          primary_keys.push_back(std::string(primary_key_node->val_));
        }
      } else {
        return DB_FAILED;
      }
    }
  }
  TableInfo *new_table = nullptr;
  if (db->catalog_mgr_->CreateTable(table_name, new Schema(columns), context->txn_, new_table) != DB_SUCCESS) {
    for (int i = 0; i < (int)columns.size(); ++i) {
      delete columns[i];
    }
    return DB_FAILED;
  }
  IndexInfo *index_info = nullptr;
  dberr_t status;
  if ((status = db->catalog_mgr_->CreateIndex(table_name, "_" + table_name + "_primkey", primary_keys, context->txn_, index_info)) != DB_SUCCESS) {
    return status;
  }
  // for (int i = 0; i < (int)columns.size(); ++i) {
  //   delete columns[i];
  // }
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteDropTable(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropTable" << std::endl;
#endif
  if (current_db_ == "") {
    return DB_FAILED;
  }
  DBStorageEngine *db = dbs_[current_db_];
  if (db == nullptr) {
    return DB_FAILED;
  }
  pSyntaxNode table_name_node = ast->child_;
  std::string table_name = table_name_node->val_;
  context->AddAffectedRows();
  return db->catalog_mgr_->DropTable(table_name);
}

dberr_t ExecuteEngine::ExecuteShowIndexes(pSyntaxNode ast, ExecuteContext *context) {

#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowIndexes" << std::endl;
#endif
  if (current_db_ == "") {
    return DB_FAILED;
  }
  DBStorageEngine *db = dbs_[current_db_];
  if (db == nullptr) {
    return DB_FAILED;
  }
  std::vector<TableInfo *> tables;
  tables.clear();
  dberr_t status;
  if ((status = db->catalog_mgr_->GetTables(tables)) != DB_SUCCESS) {
    LOG(INFO)<<"ExecuteShowIndexes: GetTables failed"<<std::endl;
    return status;
  }
  std::vector<IndexInfo *> indexes;
  indexes.clear();
  for (auto tableInfo: tables) {
    if ((status = db->catalog_mgr_->GetTableIndexes(tableInfo->GetTableName(), indexes)) != DB_SUCCESS) {
      LOG(INFO)<<"ExecuteShowIndexes: GetTableIndexes failed"<<std::endl;
      return status;
    }
  }
  unsigned int max_len = 7;
  for (int i = 0; i < (int)indexes.size(); ++i) {
    max_len = max(max_len, (unsigned int)indexes[i]->GetIndexName().length());
  }
  context->PrintDivider(max_len);
  context->PrintString("Indexes", max_len);
  context->PrintDivider(max_len);
  for (int i = 0; i < (int)indexes.size(); ++i) {
    context->PrintString(indexes[i]->GetIndexName(), max_len);
    context->AddNumSelectedRows();
  }
  context->PrintDivider(max_len);
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteCreateIndex(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateIndex" << std::endl;
#endif
  if (current_db_ == "") {
    return DB_FAILED;
  }
  DBStorageEngine *db = dbs_[current_db_];
  if (db == nullptr) {
    return DB_FAILED;
  }
  pSyntaxNode index_name_node = ast->child_;
  pSyntaxNode table_name_node = index_name_node->next_;
  pSyntaxNode index_keys_node = table_name_node->next_;
  std::string index_name = index_name_node->val_;
  std::string table_name = table_name_node->val_;
  std::vector<std::string> index_keys;
  TableInfo *tableInfo = nullptr;
  if (db->catalog_mgr_->GetTable(table_name, tableInfo) != DB_SUCCESS) {
    return DB_FAILED;
  }
  Schema *schema = tableInfo->GetSchema();
  int flag = 0;
  for (pSyntaxNode index_key_node = index_keys_node->child_; index_key_node != nullptr; index_key_node = index_key_node->next_) {
    std::string key_name = index_key_node->val_;
    index_keys.push_back(key_name);
    uint32_t key_id = 0;
    schema->GetColumnIndex(key_name, key_id);
    const Column *col = schema->GetColumn(key_id);
    if (col->IsUnique()) flag = 1;
  }
  if (flag == 0) {
    std::cout << "Cannot create index on non-unique attributes!" << std::endl;
    return DB_FAILED;
  }
  IndexInfo *index_info = nullptr;
  dberr_t status;
  if ((status = db->catalog_mgr_->CreateIndex(table_name, index_name, index_keys, context->txn_, index_info)) != DB_SUCCESS) {
    return status;
  }

  // insert tuples into index
  /*
  TableInfo *tableInfo = nullptr;
  if (db->catalog_mgr_->GetTable(table_name, tableInfo) != DB_SUCCESS) {
    return DB_FAILED;
  }
  std::vector<int> indexColumns;
  indexColumns.clear();
  Schema *schema = tableInfo->GetSchema();
  dberr_t err;
  for (int i = 0; i < (int)index_keys.size(); ++i) {
    uint32_t ind = 0;
    if ((err = schema->GetColumnIndex(index_keys[i], ind)) != DB_SUCCESS) {
      return err;
    }
    indexColumns.push_back(ind);
  }
  for (auto it = tableInfo->GetTableHeap()->Begin(context->txn_); it != tableInfo->GetTableHeap()->End(); it ++) {
    std::vector<Field> row;
    for (int i = 0; i < (int)indexColumns.size(); ++i) {
      row.push_back(*it->GetField(indexColumns[i]));
    }
    Row keyRow(row);
    if((err = index_info->GetIndex()->InsertEntry(keyRow, it->GetRowId(), context->txn_)) != DB_SUCCESS) {
      std::cerr << "Duplicate key" << std::endl;
      db->catalog_mgr_->DropIndex(table_name, index_name);
      return err;
    }
  }
  */
  
  context->AddAffectedRows();
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteDropIndex(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropIndex" << std::endl;
#endif
  if (current_db_ == "") {
    return DB_FAILED;
  }
  DBStorageEngine *db = dbs_[current_db_];
  if (db == nullptr) {
    return DB_FAILED;
  }
  pSyntaxNode index_name_node = ast->child_;
  std::string index_name = index_name_node->val_;
  vector<TableInfo *> tables;
  dberr_t status;
  if ((status = db->catalog_mgr_->GetTables(tables)) != DB_SUCCESS) {
    return status;
  }
  for (int i = 0; i < (int)tables.size(); ++i) {
    vector<IndexInfo *> indexes;
    indexes.clear();
    if ((status = db->catalog_mgr_->GetTableIndexes(tables[i]->GetTableName(), indexes)) != DB_SUCCESS) {
      return status;
    }
    for (int j = 0; j < (int)indexes.size(); ++j) {
      if (indexes[j]->GetIndexName() == index_name) {
        context->AddAffectedRows();
        return db->catalog_mgr_->DropIndex(tables[i]->GetTableName(), indexes[j]->GetIndexName());
      }
    }
  }
  return DB_INDEX_NOT_FOUND;
}

dberr_t ExecuteEngine::ExecuteSelect(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteSelect" << std::endl;
#endif
  if (current_db_ == "") {
    return DB_FAILED;
  }
  DBStorageEngine *db = dbs_[current_db_];
  if (db == nullptr) {
    return DB_FAILED;
  }
  pSyntaxNode select_node = ast->child_;
  pSyntaxNode from_node = select_node->next_;
  pSyntaxNode where_node = from_node->next_;
  dberr_t status;
  std::string tableName = std::string(from_node->val_);
  TableInfo *tableInfo = nullptr;
  if ((status = db->catalog_mgr_->GetTable(tableName, tableInfo)) != DB_SUCCESS) {
    return status;
  }
  std::vector<IndexInfo *> indexes;
  indexes.clear();
  if ((status = db->catalog_mgr_->GetTableIndexes(tableName, indexes)) != DB_SUCCESS) {
    return status;
  }
  std::vector<uint32_t> selectIdx;
  if (select_node->type_ == kNodeAllColumns) {
    for (int i = 0; i < (int)tableInfo->GetSchema()->GetColumnCount(); ++i) {
      selectIdx.push_back(i);
    }
  } else if (select_node->type_ == kNodeColumnList) {
    for (pSyntaxNode select_col_node = select_node->child_; select_col_node != nullptr; select_col_node = select_col_node->next_) {
      uint32_t ind = 0;
      if ((status = tableInfo->GetSchema()->GetColumnIndex(std::string(select_col_node->val_), ind)) != DB_SUCCESS) {
        return status;
      }
      selectIdx.push_back(ind);
    }
  } else {
    return DB_FAILED;
  }
  IndexInfo *pindex = nullptr;
  std::vector<Field> keyRow;
  context->err_ = DB_SUCCESS;
  bool useIndex = ChooseIndex(tableInfo->GetSchema(), indexes, where_node, context, pindex, keyRow);
  // LOG(INFO) << "use index: " << useIndex << std::endl;
  // if(pindex) LOG(INFO) << "index name: " << pindex->GetIndexName() << std::endl;
  if (context->err_ != DB_SUCCESS) {
    return context->err_;
  }
  if (!useIndex) {
    std::vector<std::string> columns;
    columns.clear();
    for (auto col: selectIdx) {
      columns.push_back(tableInfo->GetSchema()->GetColumn(col)->GetName());
    }
    context->SetHeader(columns);
    int fieldCnt = columns.size();
    for (auto it = tableInfo->GetTableHeap()->Begin(context->txn_); it != tableInfo->GetTableHeap()->End(); it ++) {
      if (where_node != nullptr) {
        if (!EvaluateWhere(where_node->child_, tableInfo->GetSchema(), &(*it), context)) {
          continue;
        }
      }
      std::vector<std::string> row;
      row.clear();
      for (int i = 0; i < fieldCnt; ++i) {
        row.push_back((*it).GetField(selectIdx[i])->GetString());
      }
      context->PrepareRow(row);
    }
    context->PrintHeader();
    for (auto it = tableInfo->GetTableHeap()->Begin(context->txn_); it != tableInfo->GetTableHeap()->End(); it ++) {
      if (where_node != nullptr) {
        context->err_ = DB_SUCCESS;
        if (!EvaluateWhere(where_node->child_, tableInfo->GetSchema(), &(*it), context)) {
          if (context->err_ != DB_SUCCESS) {
            return context->err_;
          }
          continue;
        }
        if (context->err_ != DB_SUCCESS) {
          return context->err_;
        }
      }
      std::vector<std::string> row;
      row.clear();
      for (int i = 0; i < fieldCnt; ++i) {
        row.push_back((*it).GetField(selectIdx[i])->GetString());
      }
      context->AddNumSelectedRows();
      context->PrintRow(row);
    }
    context->PrintTableDivider();
    return DB_SUCCESS;
  } else {
    std::vector<std::string> columns;
    columns.clear();
    for (auto col: selectIdx) {
      columns.push_back(tableInfo->GetSchema()->GetColumn(col)->GetName());
    }
    context->SetHeader(columns);
    int fieldCnt = columns.size();

    Row key(keyRow);
    std::vector<RowId> res;
    pindex->GetIndex()->ScanKey(key, res, context->txn_);

    std::vector<std::string> prow;
    prow.clear();
    
    for (auto rowId: res) {
      Row row(rowId);
      tableInfo->GetTableHeap()->GetTuple(&row, context->txn_);
      if (where_node != nullptr) {
        if (!EvaluateWhere(where_node->child_, tableInfo->GetSchema(), &row, context)) {
          continue;
        }
      }
      prow.clear();
      for (int i = 0; i < fieldCnt; ++i) {
        prow.push_back(row.GetField(selectIdx[i])->GetString());
      }
      context->PrepareRow(prow);
    }

    context->PrintHeader();
    if (prow.size() > 0) {
      context->AddNumSelectedRows();
      context->PrintRow(prow);
    }
    context->PrintTableDivider();
  }
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteInsert(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteInsert" << std::endl;
#endif
  if (current_db_ == "") {
    return DB_FAILED;
  }
  DBStorageEngine *db = dbs_[current_db_];
  if (db == nullptr) {
    return DB_FAILED;
  }
  pSyntaxNode insert_node = ast->child_;
  pSyntaxNode values_node = insert_node->next_;
  std::string tableName = std::string(insert_node->val_);
  TableInfo *tableInfo = nullptr;
  dberr_t status;
  if ((status = db->catalog_mgr_->GetTable(tableName, tableInfo)) != DB_SUCCESS) {
    LOG(INFO) << "Table not found" << std::endl;
    return status;
  }
  Schema *schema = tableInfo->GetSchema();
  int columnCnt = schema->GetColumnCount();
  std::vector<Field> columns;
  columns.clear();
  pSyntaxNode value_node = values_node->child_;
  for (int i = 0; i < columnCnt; ++i) {
    if (value_node == nullptr) {
      LOG(INFO) << "Number of values incorrect" << std::endl;
      return DB_FAILED;
    }
    TypeId type = schema->GetColumn(i)->GetType();
    if (value_node->type_ == kNodeNull) {
      if (schema->GetColumn(i)->IsNullable()) {
        columns.push_back(Field(type));
      } else {
        LOG(INFO) << "Not nullable" << std::endl;
        return DB_FAILED;
      }
    } else if (type == TypeId::kTypeInt) {
      if (value_node->type_ != kNodeNumber) {
        std::cerr << "Wrong type" << std::endl;
        return DB_FAILED;
      }
      columns.push_back(Field(type, std::stoi(value_node->val_)));
    } else if (type == TypeId::kTypeFloat) {
      if (value_node->type_ != kNodeNumber) {
        std::cerr << "Wrong type" << std::endl;
        return DB_FAILED;
      }
      columns.push_back(Field(type, std::stof(value_node->val_)));
    } else if (type == TypeId::kTypeChar) {
      if (value_node->type_ != kNodeString) {
        std::cerr << "Wrong type" << std::endl;
        return DB_FAILED;
      }
      if (strlen(value_node->val_) != schema->GetColumn(i)->GetLength()) {
        LOG(INFO) << "String length incorrect" << std::endl;
        return DB_FAILED;
      }
      columns.push_back(Field(type, value_node->val_, strlen(value_node->val_), true));
    } else {
      LOG(INFO) << "Wrong type" << std::endl;
      return DB_FAILED;
    }
    value_node = value_node->next_;
  }
  if (value_node != nullptr) {
    LOG(INFO) << "Number of values incorrect" << std::endl;
    return DB_FAILED;
  }

  // judge unique
  std::vector<IndexInfo *> indexes;
  indexes.clear();
  db->catalog_mgr_->GetTableIndexes(tableName, indexes);
  /*
  for (int i = 0; i < columnCnt; ++i) {
    if (schema->GetColumn(i)->IsUnique()) {
      int hasIndex = 0;
      for (auto index: indexes) {
        IndexSchema *indSchema = index->GetIndexKeySchema();
        if (indSchema->GetColumnCount() == 1 && indSchema->GetColumn(0)->GetName() == schema->GetColumn(i)->GetName()) {
          hasIndex = 1;
          break;
        }
      }
      if (hasIndex) continue;
      // no index
      for (auto it = tableInfo->GetTableHeap()->Begin(context->txn_); it != tableInfo->GetTableHeap()->End(); it ++) {
        if (it->GetField(i)->CompareEquals(columns[i])) {
          std::cerr << "Duplicate key on " << schema->GetColumn(i)->GetName() << std::endl;
          return DB_FAILED;
        }
      }
    }
  }
  */

  // judge unique in index
  /*
  for (auto index: indexes) {
    IndexSchema *indSchema = index->GetIndexKeySchema();
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
      key.push_back(columns[indexColumns[i]]);
    }
    Row keyRow(key);
    std::vector<RowId> res;
    if (index->GetIndex()->ScanKey(keyRow, res, context->txn_) != DB_KEY_NOT_FOUND) {
      std::cerr << "Duplicate in index " << index->GetIndexName() << std::endl;
      return DB_FAILED;
    }
  }
  */
  Row row(columns);
  if (!tableInfo->GetTableHeap()->InsertTuple(row, context->txn_)) {
    // LOG(INFO) << "Insertion failed" << std::endl;
    return DB_FAILED;
  }

  // insert into index
  for (auto index: indexes) {
    IndexSchema *indSchema = index->GetIndexKeySchema();
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
      key.push_back(columns[indexColumns[i]]);
    }
    Row keyRow(key);
    if ((status = index->GetIndex()->InsertEntry(keyRow, row.GetRowId(), context->txn_)) != DB_SUCCESS) {
      tableInfo->GetTableHeap()->MarkDelete(row.GetRowId(), context->txn_);
      tableInfo->GetTableHeap()->ApplyDelete(row.GetRowId(), context->txn_);
      // std::cout << "Duplicate Key" << std::endl;
      // LOG(INFO) << "Insertion failed" << std::endl;
      return status;
    }
  }
  context->AddAffectedRows();
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteDelete(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDelete" << std::endl;
#endif
  if (current_db_ == "") {
    return DB_FAILED;
  }
  DBStorageEngine *db = dbs_[current_db_];
  if (db == nullptr) {
    return DB_FAILED;
  }
  pSyntaxNode delete_node = ast->child_;
  pSyntaxNode where_node = delete_node->next_;
  std::string tableName = delete_node->val_;
  dberr_t status;
  TableInfo *tableInfo = nullptr;
  if ((status = db->catalog_mgr_->GetTable(tableName, tableInfo)) != DB_SUCCESS) {
    return status;
  }
  std::vector<IndexInfo *> indexes;
  indexes.clear();
  if ((status = db->catalog_mgr_->GetTableIndexes(tableName, indexes)) != DB_SUCCESS) {
    return status;
  }
  IndexInfo *pindex = nullptr;
  std::vector<Field> keyRow;
  context->err_ = DB_SUCCESS;
  bool useIndex = ChooseIndex(tableInfo->GetSchema(), indexes, where_node, context, pindex, keyRow);
  // LOG(INFO) << "use index: " << useIndex << std::endl;
  // if(pindex) LOG(INFO) << "index name: " << pindex->GetIndexName() << std::endl;
  if (context->err_ != DB_SUCCESS) {
    return context->err_;
  }
  Schema *schema = tableInfo->GetSchema();
  if (!useIndex) {
    // int fieldCnt = schema->GetColumnCount();
    for (auto it = tableInfo->GetTableHeap()->Begin(context->txn_); it != tableInfo->GetTableHeap()->End(); it ++) {
      if (where_node != nullptr) {
        if (!EvaluateWhere(where_node->child_, schema, &(*it), context)) {
          continue;
        }
      }
      if(!tableInfo->GetTableHeap()->MarkDelete(it->GetRowId(), context->txn_)) {
        LOG(INFO) << "MarkDelete failed" << std::endl;
      }
      if (where_node != nullptr) {
        for (auto indexInfo: indexes) {
          std::vector<Field> keys;
          keys.clear();
          for (int i = 0; i < (int)indexInfo->GetIndexKeySchema()->GetColumnCount(); ++i) {
            uint32_t ind = 0;
            if((status = schema->GetColumnIndex(indexInfo->GetIndexKeySchema()->GetColumn(i)->GetName(), ind)) != DB_SUCCESS) {
              return status;
            }
            keys.push_back(*(it->GetField(ind)));
          }
          Row keyRow(keys);
          // keyRow->SetRowId(it->GetRowId());
          if((status = indexInfo->GetIndex()->RemoveEntry(keyRow, it->GetRowId(), context->txn_)) != DB_SUCCESS) {
            LOG(INFO) << "RemoveEntry failed" << std::endl;
          }
        }
      }
      tableInfo->GetTableHeap()->ApplyDelete(it->GetRowId(), context->txn_);
      context->AddAffectedRows();
    }
    if (where_node == nullptr) {
      for (auto indexInfo: indexes) {
        if ((status = indexInfo->GetIndex()->Destroy()) != DB_SUCCESS) {
          return status;
        }
      }
    }
    return DB_SUCCESS;
  } else {
    Row key(keyRow);
    std::vector<RowId> res;
    pindex->GetIndex()->ScanKey(key, res, context->txn_);
    
    for (auto rowId: res) {
      Row row(rowId);
      tableInfo->GetTableHeap()->GetTuple(&row, context->txn_);
      if (where_node != nullptr) {
        if (!EvaluateWhere(where_node->child_, schema, &row, context)) {
          continue;
        }
      }
      if(!tableInfo->GetTableHeap()->MarkDelete(rowId, context->txn_)) {
        LOG(INFO) << "MarkDelete failed" << std::endl;
      }
      for (auto indexInfo: indexes) {
        std::vector<Field> keys;
        keys.clear();
        for (int i = 0; i < (int)indexInfo->GetIndexKeySchema()->GetColumnCount(); ++i) {
          uint32_t ind = 0;
          if((status = schema->GetColumnIndex(indexInfo->GetIndexKeySchema()->GetColumn(i)->GetName(), ind)) != DB_SUCCESS) {
            return status;
          }
          keys.push_back(*row.GetField(ind));
        }
        Row keyRow(keys);
        if((status = indexInfo->GetIndex()->RemoveEntry(keyRow, rowId, context->txn_)) != DB_SUCCESS) {
          LOG(INFO) << "RemoveEntry failed" << std::endl;
        }
      }
      tableInfo->GetTableHeap()->ApplyDelete(rowId, context->txn_);
      context->AddAffectedRows();
    }
  }
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteUpdate(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteUpdate" << std::endl;
#endif
  if (current_db_ == "") {
    return DB_FAILED;
  }
  DBStorageEngine *db = dbs_[current_db_];
  if (db == nullptr) {
    return DB_FAILED;
  }
  pSyntaxNode update_node = ast->child_;
  pSyntaxNode set_node = update_node->next_;
  pSyntaxNode where_node = set_node->next_;
  std::string tableName = update_node->val_;
  dberr_t status;
  TableInfo *tableInfo = nullptr;
  if ((status = db->catalog_mgr_->GetTable(tableName, tableInfo)) != DB_SUCCESS) {
    return status;
  }
  std::vector<IndexInfo *> indexes;
  indexes.clear();
  if ((status = db->catalog_mgr_->GetTableIndexes(tableName, indexes)) != DB_SUCCESS) {
    return status;
  }
  IndexInfo *pindex = nullptr;
  std::vector<Field> keyRow;
  context->err_ = DB_SUCCESS;
  bool useIndex = ChooseIndex(tableInfo->GetSchema(), indexes, where_node, context, pindex, keyRow);
  LOG(INFO) << "use index: " << useIndex << std::endl;
  if(pindex) LOG(INFO) << "index name: " << pindex->GetIndexName() << std::endl;
  if (context->err_ != DB_SUCCESS) {
    return context->err_;
  }
  Schema *schema = tableInfo->GetSchema();
  if (!useIndex) {
    // int fieldCnt = schema->GetColumnCount();
    for (auto it = tableInfo->GetTableHeap()->Begin(context->txn_); it != tableInfo->GetTableHeap()->End(); it ++) {
      if (where_node != nullptr) {
        if (!EvaluateWhere(where_node->child_, schema, &(*it), context)) {
          continue;
        }
      }
      std::vector<Field> newColumns;
      newColumns.clear();
      for (int i = 0; i < (int)schema->GetColumnCount(); ++ i) {
        newColumns.push_back(*(it->GetField(i)));
      }
      for (pSyntaxNode set_node_item = set_node->child_; set_node_item != nullptr; set_node_item = set_node_item->next_) {
        uint32_t ind = 0;
        pSyntaxNode set_column_node = set_node_item->child_;
        pSyntaxNode set_value_node = set_column_node->next_;
        if((status = schema->GetColumnIndex(set_column_node->val_, ind)) != DB_SUCCESS) {
          return status;
        }
        TypeId type = schema->GetColumn(ind)->GetType();
        if (set_value_node->type_ == kNodeNull) {
          if (schema->GetColumn(ind)->IsNullable()) {
            newColumns[ind] = Field(type);
          } else {
            return DB_FAILED;
          }
        } else {
          switch (type) {
            case TypeId::kTypeInt:
              if (set_value_node->type_ != kNodeNumber) {
                return DB_FAILED;
              }
              newColumns[ind] = Field(type, std::stoi(set_value_node->val_));
              break;
            case TypeId::kTypeFloat:
              if (set_value_node->type_ != kNodeNumber) {
                return DB_FAILED;
              }
              newColumns[ind] = Field(type, std::stof(set_value_node->val_));
              break;
            case TypeId::kTypeChar:
              if (set_value_node->type_ != kNodeString) {
                return DB_FAILED;
              }
              newColumns[ind] = Field(type, set_value_node->val_, strlen(set_value_node->val_), true);
              break;
            default:
              LOG(INFO) << "Unsupported type" << std::endl;
              return DB_FAILED;
          }
        }
      }
      Row newRow(newColumns);
      for (auto indexInfo: indexes) {
        std::vector<Field> keys;
        keys.clear();
        for (int i = 0; i < (int)indexInfo->GetIndexKeySchema()->GetColumnCount(); ++i) {
          uint32_t ind = 0;
          if((status = schema->GetColumnIndex(indexInfo->GetIndexKeySchema()->GetColumn(i)->GetName(), ind)) != DB_SUCCESS) {
            return status;
          }
          keys.push_back(*(it->GetField(ind)));
        }
        Row keyRow(keys);
        // keyRow->SetRowId(it->GetRowId());
        if((status = indexInfo->GetIndex()->RemoveEntry(keyRow, it->GetRowId(), context->txn_)) != DB_SUCCESS) {
          LOG(INFO) << "RemoveEntry failed" << std::endl;
        }
      }
      if(!tableInfo->GetTableHeap()->UpdateTuple(newRow, it->GetRowId(), context->txn_)) {
        LOG(INFO) << "UpdateTuple failed" << std::endl;
      }
      for (auto indexInfo: indexes) {
        std::vector<Field> keys;
        keys.clear();
        for (int i = 0; i < (int)indexInfo->GetIndexKeySchema()->GetColumnCount(); ++i) {
          uint32_t ind = 0;
          if((status = schema->GetColumnIndex(indexInfo->GetIndexKeySchema()->GetColumn(i)->GetName(), ind)) != DB_SUCCESS) {
            return status;
          }
          keys.push_back(newColumns[ind]);
        }
        Row keyRow(keys);
        // keyRow->SetRowId(it->GetRowId());
        if((status = indexInfo->GetIndex()->InsertEntry(keyRow, it->GetRowId(), context->txn_)) != DB_SUCCESS) {
          LOG(INFO) << "InsertEntry failed" << std::endl;
          ASSERT(false, "Update duplicate key");
        }
      }
      context->AddAffectedRows();
    }
    return DB_SUCCESS;
  } else {
    Row key(keyRow);
    std::vector<RowId> res;
    pindex->GetIndex()->ScanKey(key, res, context->txn_);
    
    for (auto rowId: res) {
      Row row(rowId);
      tableInfo->GetTableHeap()->GetTuple(&row, context->txn_);
      if (where_node != nullptr) {
        if (!EvaluateWhere(where_node->child_, schema, &row, context)) {
          continue;
        }
      }
      std::vector<Field> newColumns;
      newColumns.clear();
      for (int i = 0; i < (int)schema->GetColumnCount(); ++ i) {
        newColumns.push_back(*row.GetField(i));
      }
      for (pSyntaxNode set_node_item = set_node->child_; set_node_item != nullptr; set_node_item = set_node_item->next_) {
        uint32_t ind = 0;
        pSyntaxNode set_column_node = set_node_item->child_;
        pSyntaxNode set_value_node = set_column_node->next_;
        if((status = schema->GetColumnIndex(set_column_node->val_, ind)) != DB_SUCCESS) {
          return status;
        }
        TypeId type = schema->GetColumn(ind)->GetType();
        if (set_value_node->type_ == kNodeNull) {
          if (schema->GetColumn(ind)->IsNullable()) {
            newColumns[ind] = Field(type);
          } else {
            return DB_FAILED;
          }
        } else {
          switch (type) {
            case TypeId::kTypeInt:
              if (set_value_node->type_ != kNodeNumber) {
                return DB_FAILED;
              }
              newColumns[ind] = Field(type, std::stoi(set_value_node->val_));
              break;
            case TypeId::kTypeFloat:
              if (set_value_node->type_ != kNodeNumber) {
                return DB_FAILED;
              }
              newColumns[ind] = Field(type, std::stof(set_value_node->val_));
              break;
            case TypeId::kTypeChar:
              if (set_value_node->type_ != kNodeString) {
                return DB_FAILED;
              }
              newColumns[ind] = Field(type, set_value_node->val_, strlen(set_value_node->val_), true);
              break;
            default:
              LOG(INFO) << "Unsupported type" << std::endl;
              return DB_FAILED;
          }
        }
      }
      Row newRow(newColumns);
      for (auto indexInfo: indexes) {
        std::vector<Field> keys;
        keys.clear();
        for (int i = 0; i < (int)indexInfo->GetIndexKeySchema()->GetColumnCount(); ++i) {
          uint32_t ind = 0;
          if((status = schema->GetColumnIndex(indexInfo->GetIndexKeySchema()->GetColumn(i)->GetName(), ind)) != DB_SUCCESS) {
            return status;
          }
          keys.push_back(*row.GetField(ind));
        }
        Row keyRow(keys);
        if((status = indexInfo->GetIndex()->RemoveEntry(keyRow, rowId, context->txn_)) != DB_SUCCESS) {
          LOG(INFO) << "RemoveEntry failed" << std::endl;
        }
      }
      tableInfo->GetTableHeap()->UpdateTuple(newRow, rowId, context->txn_);
      for (auto indexInfo: indexes) {
        std::vector<Field> keys;
        keys.clear();
        for (int i = 0; i < (int)indexInfo->GetIndexKeySchema()->GetColumnCount(); ++i) {
          uint32_t ind = 0;
          if((status = schema->GetColumnIndex(indexInfo->GetIndexKeySchema()->GetColumn(i)->GetName(), ind)) != DB_SUCCESS) {
            return status;
          }
          keys.push_back(newColumns[ind]);
        }
        Row keyRow(keys);
        if((status = indexInfo->GetIndex()->InsertEntry(keyRow, rowId, context->txn_)) != DB_SUCCESS) {
          LOG(INFO) << "InsertEntry failed" << std::endl;
          ASSERT(false, "Update duplicate key");
        }
      }
      context->AddAffectedRows();
    }
  }
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteTrxBegin(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxBegin" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteTrxCommit(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxCommit" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteTrxRollback(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxRollback" << std::endl;
#endif
  return DB_FAILED;
}


extern "C" {
int yyparse(void);
// FILE *yyin2;
#include "parser/minisql_lex.h"
#include "parser/parser.h"
}

dberr_t ExecuteEngine::ExecuteExecfile(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteExecfile" << std::endl;
#endif
  // command buffer
  const int buf_size = 2048;
  char cmd[buf_size];
  // for print syntax tree
  // TreeFileManagers syntax_tree_file_mgr("syntax_tree_");
  // [[maybe_unused]] uint32_t syntax_tree_id = 0;

  FILE *fp = fopen(ast->child_->val_, "r");
  if (fp == nullptr) {
    LOG(INFO) << "ExecuteExecfile: open file failed" << std::endl;
    return DB_FAILED;
  }

  // int counter = 0;
  while (1) {
    // read from buffer
    InputCommand(cmd, buf_size, fp);
    if(strlen(cmd) == 0) {
      break;
    }
    // ++ counter;
    // if (counter % 100 == 0) {
    //   LOG(INFO) << counter << std::endl;
    // }
    // create buffer for sql input
    YY_BUFFER_STATE bp = yy_scan_string(cmd);
    if (bp == nullptr) {
      LOG(ERROR) << "Failed to create yy buffer state." << std::endl;
      exit(1);
    }
    yy_switch_to_buffer(bp);

    // init parser module
    MinisqlParserInit();

    // parse
    yyparse();

    // parse result handle
    if (MinisqlParserGetError()) {
      // error
      printf("%s\n", MinisqlParserGetErrorMessage());
    } else {
// #ifdef ENABLE_PARSER_DEBUG
      // printf("[INFO] Sql syntax parse ok!\n");
      // SyntaxTreePrinter printer(MinisqlGetParserRootNode());
      // printer.PrintTree(syntax_tree_file_mgr[syntax_tree_id++]);
// #endif
    }

    ExecuteContext context;
    context.disablePrint_ = true;
    this->Execute(MinisqlGetParserRootNode(), &context);
    // sleep(1);

    // clean memory after parse
    MinisqlParserFinish();
    yy_delete_buffer(bp);
    yylex_destroy();

    // quit condition
    if (context.flag_quit_) {
      printf("bye!\n");
      break;
    }

  }

  fclose(fp);

  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteQuit(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteQuit" << std::endl;
#endif
  ASSERT(ast->type_ == kNodeQuit, "Unexpected node type.");
  context->flag_quit_ = true;
  return DB_SUCCESS;
}

bool ExecuteEngine::ChooseIndex(Schema *schema, std::vector<IndexInfo *> &indexes, pSyntaxNode where_node, ExecuteContext *context, IndexInfo *&pindex, std::vector<Field> &keyRow) {
  if (where_node == nullptr) {
    pindex = nullptr;
    return false;
  } else {
    std::vector<int> statusColumns;
    // 0 - 没出现
    // 1 - 等值
    // 2 - 范围 < <= > >=
    // 4 - 不可用
    std::vector<Field> conditions;
    statusColumns.clear();
    conditions.clear();
    int attrCount = schema->GetColumnCount();
    for (int i = 0; i < attrCount; ++ i) {
      statusColumns.push_back(0);
      conditions.push_back(Field(schema->GetColumn(i)->GetType()));
    }
    EvaluateIndex(where_node->child_, schema, statusColumns, conditions, context);
    std::vector<int> statusIndexes;
    // 0 - 不用
    // 1 - 等值
    // 2 - 范围
    statusIndexes.clear();
    for (int i = 0; i < (int)indexes.size(); ++i) {
      int res = -1;
      IndexSchema *indexSchema = indexes[i]->GetIndexKeySchema();
      for (auto j: indexSchema->GetColumns()) {
        uint32_t ind = 0;
        schema->GetColumnIndex(j->GetName(), ind);
        if (statusColumns[ind] & 4) {
          res = 0;
          break;
        } else if (statusColumns[ind] & 1) {
          if (res != 2 && res != 0) res = 1;
          else if (res == 2) {
            res = 0;
            break;
          }
        } else if (statusColumns[ind] & 2) {
          if (res != 2 && res != 0) res = 2;
          else if (res == 2) {
            res = 0;
            break;
          }
        }
      }
      if (res == -1) res = 0;
      statusIndexes.push_back(res);
    }
    for (int i = 0; i < (int)statusIndexes.size(); ++i) {
      if (statusIndexes[i] == 1) {
      IndexSchema *indexSchema = indexes[i]->GetIndexKeySchema();
        pindex = indexes[i];
        keyRow.clear();
        for (int j = 0; j < (int)indexSchema->GetColumnCount(); ++j) {
          uint32_t ind = 0;
          schema->GetColumnIndex(indexSchema->GetColumn(j)->GetName(), ind);
          keyRow.push_back(conditions[ind]);
        }
        return true;
      }
    }
    pindex = nullptr;
    return false;
  }
}

void ExecuteEngine::EvaluateIndex(pSyntaxNode where_node, Schema *schema, std::vector<int> &statusColumns, std::vector<Field> &conditions, ExecuteContext *context) {
  if (where_node == nullptr) return;
  if (where_node->type_ == kNodeConnector) {
    if (strcmp(where_node->val_, "and") == 0) {
      EvaluateIndex(where_node->child_, schema, statusColumns, conditions, context);
      EvaluateIndex(where_node->child_->next_, schema, statusColumns, conditions, context);
    } else if (strcmp(where_node->val_, "or") == 0) {
      for (int i = 0; i < (int)statusColumns.size(); ++ i) {
        statusColumns[i] |= 4;
      }
    }
    return;
  } else if (where_node->type_ == kNodeCompareOperator) {
    pSyntaxNode leftNode = where_node->child_;
    std::string leftName = leftNode->val_;
    dberr_t err = DB_SUCCESS;
    uint32_t columnIndex;
    if ((err = schema->GetColumnIndex(leftName, columnIndex)) != DB_SUCCESS) {
      context->err_ = err;
      return;
    }
    if (strcmp(where_node->val_, "is") == 0) {
      statusColumns[columnIndex] |= 4;
    } else if (strcmp(where_node->val_, "not") == 0) {
      statusColumns[columnIndex] |= 4;
    } else if (strcmp(where_node->val_, "=") == 0) {
      statusColumns[columnIndex] |= 1;
      pSyntaxNode rightNode = where_node->child_->next_;
      TypeId type = schema->GetColumn(columnIndex)->GetType();
      switch (type) {
        case TypeId::kTypeInt:
          if (rightNode->type_ != kNodeNumber) {
            context->err_ = DB_FAILED;
            return;
          }
          conditions[columnIndex] = Field(type, std::stoi(rightNode->val_));
          break;
        case TypeId::kTypeFloat:
          if (rightNode->type_ != kNodeNumber) {
            context->err_ = DB_FAILED;
            return;
          }
          conditions[columnIndex] = Field(type, std::stof(rightNode->val_));
          break;
        case TypeId::kTypeChar:
          if (rightNode->type_ != kNodeString) {
            context->err_ = DB_FAILED;
            return;
          }
          conditions[columnIndex] = Field(type, rightNode->val_, strlen(rightNode->val_), true);
          break;
        default:
          context->err_ = DB_FAILED;
          LOG(INFO) << "Unsupported type" << std::endl;
          return;
      }
    } else if (strcmp(where_node->val_, "<>") == 0) {
      statusColumns[columnIndex] |= 4;
    } else if (strcmp(where_node->val_, "<") == 0) {
      statusColumns[columnIndex] |= 2;
    } else if (strcmp(where_node->val_, "<=") == 0) {
      statusColumns[columnIndex] |= 2;
    } else if (strcmp(where_node->val_, ">") == 0) {
      statusColumns[columnIndex] |= 2;
    } else if (strcmp(where_node->val_, ">=") == 0) {
      statusColumns[columnIndex] |= 2;
    } else {
      statusColumns[columnIndex] |= 4;
      context->err_ = DB_FAILED;
    }
    return;
  }
}

bool ExecuteEngine::EvaluateWhere(pSyntaxNode where_node, const Schema *schema, const Row *row, ExecuteContext *context) {
  if (where_node == nullptr) {
    return true;
  }
  if (where_node->type_ == kNodeConnector) {
    if (strcmp(where_node->val_, "and") == 0) {
      return EvaluateWhere(where_node->child_, schema, row, context)
        & EvaluateWhere(where_node->child_->next_, schema, row, context);
    } else if (strcmp(where_node->val_, "or") == 0) {
      return EvaluateWhere(where_node->child_, schema, row, context)
        | EvaluateWhere(where_node->child_->next_, schema, row, context);
    } else {
      context->err_ = DB_FAILED;
      return false;
    }
  } else if (where_node->type_ == kNodeCompareOperator) {
    pSyntaxNode leftNode = where_node->child_;
    pSyntaxNode rightNode = where_node->child_->next_;
    std::string leftName = leftNode->val_;
    dberr_t err = DB_SUCCESS;
    uint32_t columnIndex;
    if ((err = schema->GetColumnIndex(leftName, columnIndex)) != DB_SUCCESS) {
      context->err_ = err;
      return false;
    }
    Field *leftField = row->GetField(columnIndex);
    if (rightNode->type_ == kNodeNull) {
      if (strcmp(where_node->val_, "is") == 0) {
        return leftField->IsNull();
      } else if (strcmp(where_node->val_, "not") == 0) {
        return !leftField->IsNull();
      } else {
        context->err_ = DB_FAILED;
        return false;
      }
    }
    TypeId type = schema->GetColumn(columnIndex)->GetType();
    Field *rightField = nullptr;
    switch (type) {
      case TypeId::kTypeInt:
        if (rightNode->type_ != kNodeNumber) {
          context->err_ = DB_FAILED;
          return false;
        }
        rightField = new Field(type, std::stoi(rightNode->val_));
        break;
      case TypeId::kTypeFloat:
        if (rightNode->type_ != kNodeNumber) {
          context->err_ = DB_FAILED;
          return false;
        }
        rightField = new Field(type, std::stof(rightNode->val_));
        break;
      case TypeId::kTypeChar:
        if (rightNode->type_ != kNodeString) {
          context->err_ = DB_FAILED;
          return false;
        }
        rightField = new Field(type, rightNode->val_, strlen(rightNode->val_), true);
        break;
      default:
        context->err_ = DB_FAILED;
        LOG(INFO) << "Unsupported type" << std::endl;
        return false;
    }
    if (strcmp(where_node->val_, "=") == 0) {
      return leftField->CompareEquals(*rightField);
    } else if (strcmp(where_node->val_, "<>") == 0) {
      return leftField->CompareNotEquals(*rightField);
    } else if (strcmp(where_node->val_, "<") == 0) {
      return leftField->CompareLessThan(*rightField);
    } else if (strcmp(where_node->val_, "<=") == 0) {
      return leftField->CompareLessThanEquals(*rightField);
    } else if (strcmp(where_node->val_, ">") == 0) {
      return leftField->CompareGreaterThan(*rightField);
    } else if (strcmp(where_node->val_, ">=") == 0) {
      return leftField->CompareGreaterThanEquals(*rightField);
    } else {
      context->err_ = DB_FAILED;
      return false;
    }
    delete rightField;
  }
  return false;
}

void ExecuteEngine::InputCommand(char *input, const int len, FILE* fp) {
  memset(input, 0, len);
  int i = 0;
  char ch;
  while ((ch = fgetc(fp)) == '\r' || ch == '\n') {
    continue;
  }
  while (ch != ';' && ch != EOF) {
    input[i++] = ch;
    ch = fgetc(fp);
  }
  if (ch == EOF) return;
  input[i] = ch;    // ;
}