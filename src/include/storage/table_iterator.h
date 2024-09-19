#ifndef MINISQL_TABLE_ITERATOR_H
#define MINISQL_TABLE_ITERATOR_H

#include "common/rowid.h"
#include "record/row.h"
#include "transaction/transaction.h"


class TableHeap;

class TableIterator {

public:
  // you may define your own constructor based on your member variables
 TableIterator(RowId id,TableHeap *table_heap_);

 TableIterator(const TableIterator &other);

  virtual ~TableIterator();

  // inline bool operator==(const TableIterator &itr) const;

  // inline bool operator!=(const TableIterator &itr) const;

  inline bool operator==(const TableIterator &itr) const {
    return this->id == itr.id && this->table_heap_ == itr.table_heap_;
  }

  inline bool operator!=(const TableIterator &itr) const {
    return !(this->id == itr.id && this->table_heap_ == itr.table_heap_);
  }

  const Row &operator*();

  Row *operator->();

  TableIterator &operator++();

  TableIterator operator++(int);

public:
  // add your own private member variables here
  RowId id;
  Row row;
  TableHeap *table_heap_;
};

#endif //MINISQL_TABLE_ITERATOR_H
