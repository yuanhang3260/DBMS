#ifndef QUERY_ITERATOR_H_
#define QUERY_ITERATOR_H_

#include <memory>
#include <vector>

#include "Database/Operation.h"
#include "Query/Common.h"

namespace DB {
class TableRecordIterator;
}

namespace Query {

class ExprTreeNode;
class SqlQuery;

class Iterator {
 public:
  Iterator(SqlQuery* query, ExprTreeNode* node);

  virtual void Init() = 0;
  virtual std::shared_ptr<FetchedResult::Tuple> GetNextTuple() = 0;

 protected:
  SqlQuery* query_ = nullptr;
  ExprTreeNode* node_ = nullptr;

  bool ready_ = false;
  bool end_ = false;
};


class PhysicalQueryIterator : public Iterator {
 public:
  PhysicalQueryIterator(SqlQuery* query, ExprTreeNode* node);

  void Init() override;
  std::shared_ptr<FetchedResult::Tuple> GetNextTuple() override;

 private:
  DB::SearchOp search_op;
  std::shared_ptr<DB::TableRecordIterator> table_iter;
};


class AndNodeIterator : public Iterator {
 public:
  AndNodeIterator(SqlQuery* query, ExprTreeNode* node);

  void Init() override;
  std::shared_ptr<FetchedResult::Tuple> GetNextTuple() override;

 private:
  ExprTreeNode* pop_node_ = nullptr;
  ExprTreeNode* other_node_ = nullptr;
};

class OrNodeIterator : public Iterator {
 public:
  OrNodeIterator(SqlQuery* query, ExprTreeNode* node);

  void Init() override;
  std::shared_ptr<FetchedResult::Tuple> GetNextTuple() override;

 private:
  FetchedResult result_;
  uint32 tuple_index_ = 0;
};

}

#endif  // QUERY_ITERATOR_H_
