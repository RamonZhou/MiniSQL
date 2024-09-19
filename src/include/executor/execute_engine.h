#ifndef MINISQL_EXECUTE_ENGINE_H
#define MINISQL_EXECUTE_ENGINE_H

#include <string>
#include <unordered_map>
#include "common/dberr.h"
#include "common/instance.h"
#include "transaction/transaction.h"
#include <chrono>

extern "C" {
#include "parser/parser.h"
};

/**
 * ExecuteContext stores all the context necessary to run in the execute engine
 * This struct is implemented by student self for necessary.
 *
 * eg: transaction info, execute result...
 */
struct ExecuteContext {
  bool flag_quit_{false};
  Transaction *txn_{nullptr};
  std::chrono::high_resolution_clock::time_point start_time_;
  bool running;
  int numAffectedRows;
  int numSelectedRows;
  std::vector<std::string> header_;
  std::vector<int> columnLengths_;
  dberr_t err_;
  bool disablePrint_;

  void AddAffectedRows(int num = 1) {
    numAffectedRows += num;
  }

  void AddNumSelectedRows(int num = 1) {
    numSelectedRows += num;
  }

  void StartRunning() {
    start_time_ = std::chrono::high_resolution_clock::now();
    numAffectedRows = numSelectedRows = 0;
    running = true;
  }

  void StopRunning(bool printTime = true) {
    if (running && printTime && !disablePrint_) {
      auto end_time = std::chrono::high_resolution_clock::now();
      auto time_span =
          std::chrono::duration_cast<std::chrono::duration<double>>(end_time - start_time_);
      printf("Execute time: %.1f ms\n\n", time_span.count() * 1000);
    }
    running = false;
  }

  void StopTimer() {
    running = false;
  }

  void PrintDivider(const unsigned int &len) const {
    if (disablePrint_) return;
    putchar('+');
    for (unsigned int i = 0; i < len + 2; i++) {
      putchar('-');
    }
    printf("+\n");
  }

  void PrintString(const std::string &str, const unsigned int &maxlen) const {
    if (disablePrint_) return;
    printf("| ");
    printf("%s", str.c_str());
    for (unsigned int i = str.size(); i < maxlen; ++i) {
      putchar(' ');
    }
    printf(" |");
    putchar('\n');
  }

  void PrintTableDivider() const {
    if (disablePrint_) return;
    putchar('+');
    for (int len: columnLengths_) {
      for (int i = 0; i < len + 2; ++ i) {
        putchar('-');
      }
      putchar('+');
    }
    putchar('\n');
  }

  void PrintField(const std::string &str, const int &id) const {
    if (disablePrint_) return;
    putchar(' ');
    printf("%s", str.c_str());
    for (int i = str.size(); i < columnLengths_[id]; ++i) {
      putchar(' ');
    }
    printf(" |");
  }

  void SetHeader(const std::vector<std::string> &header) {
    if (disablePrint_) return;
    header_.clear();
    columnLengths_.clear();
    for (int i = 0; i < (int)header.size(); ++ i) {
      header_.push_back(header[i]);
      columnLengths_.push_back(header[i].size());
    }
  }

  void PrepareRow(const std::vector<std::string> &row) {
    if (disablePrint_) return;
    for (int i = 0; i < (int)row.size(); ++ i) {
      if ((int)row[i].size() > columnLengths_[i]) {
        columnLengths_[i] = row[i].size();
      }
    }
  }

  void PrintHeader() const {
    if (disablePrint_) return;
    if (header_.empty()) {
      return;
    }
    PrintTableDivider();
    putchar('|');
    for (int i = 0; i < (int)header_.size(); ++ i) {
      PrintField(header_[i], i);
    }
    putchar('\n');
    PrintTableDivider();
  }

  void PrintRow(const std::vector<std::string> &row) const {
    if (disablePrint_) return;
    if (row.empty()) {
      return;
    }
    putchar('|');
    for (int i = 0; i < (int)row.size(); ++ i) {
      PrintField(row[i], i);
    }
    putchar('\n');
  }

  void PrintResult(const dberr_t &err, bool affected) const {
    if (disablePrint_) return;
    if (err == DB_SUCCESS) {
      if (affected) printf("OK, %d rows affected.\n", numAffectedRows);
      else printf("%d rows in total.\n", numSelectedRows);
    } else printf("Failed.\n");
  }
};

/**
 * ExecuteEngine
 */
class ExecuteEngine {
public:
  ExecuteEngine();

  ~ExecuteEngine() {
    for (auto it : dbs_) {
      delete it.second;
    }
  }

  /**
   * executor interface
   */
  dberr_t Execute(pSyntaxNode ast, ExecuteContext *context);

private:
  dberr_t ExecuteCreateDatabase(pSyntaxNode ast, ExecuteContext *context);

  dberr_t ExecuteDropDatabase(pSyntaxNode ast, ExecuteContext *context);

  dberr_t ExecuteShowDatabases(pSyntaxNode ast, ExecuteContext *context);

  dberr_t ExecuteUseDatabase(pSyntaxNode ast, ExecuteContext *context);

  dberr_t ExecuteShowTables(pSyntaxNode ast, ExecuteContext *context);

  dberr_t ExecuteCreateTable(pSyntaxNode ast, ExecuteContext *context);

  dberr_t ExecuteDropTable(pSyntaxNode ast, ExecuteContext *context);

  dberr_t ExecuteShowIndexes(pSyntaxNode ast, ExecuteContext *context);

  dberr_t ExecuteCreateIndex(pSyntaxNode ast, ExecuteContext *context);

  dberr_t ExecuteDropIndex(pSyntaxNode ast, ExecuteContext *context);

  dberr_t ExecuteSelect(pSyntaxNode ast, ExecuteContext *context);

  dberr_t ExecuteInsert(pSyntaxNode ast, ExecuteContext *context);

  dberr_t ExecuteDelete(pSyntaxNode ast, ExecuteContext *context);

  dberr_t ExecuteUpdate(pSyntaxNode ast, ExecuteContext *context);

  dberr_t ExecuteTrxBegin(pSyntaxNode ast, ExecuteContext *context);

  dberr_t ExecuteTrxCommit(pSyntaxNode ast, ExecuteContext *context);

  dberr_t ExecuteTrxRollback(pSyntaxNode ast, ExecuteContext *context);

  dberr_t ExecuteExecfile(pSyntaxNode ast, ExecuteContext *context);

  dberr_t ExecuteQuit(pSyntaxNode ast, ExecuteContext *context);

private:
  [[maybe_unused]] std::unordered_map<std::string, DBStorageEngine *> dbs_;  /** all opened databases */
  [[maybe_unused]] std::string current_db_;  /** current database */

  bool ChooseIndex(Schema *schema, std::vector<IndexInfo *> &indexes, pSyntaxNode where_node, ExecuteContext *context, IndexInfo *&pindex, std::vector<Field> &keyRow);

  void EvaluateIndex(pSyntaxNode where_node, Schema *schema, std::vector<int> &statusColumns, std::vector<Field> &conditions, ExecuteContext *context);

  bool EvaluateWhere(pSyntaxNode where_node, const Schema *schema, const Row *row, ExecuteContext *context);

  void InputCommand(char *input, const int len, FILE* fp);
};

#endif //MINISQL_EXECUTE_ENGINE_H
