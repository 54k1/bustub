
#include <cstdio>
#include <memory>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include "src/include/buffer/buffer_pool_manager.h"
#include "src/include/catalog/table_generator.h"
#include "src/include/concurrency/transaction_manager.h"
#include "src/include/execution/executor_context.h"
#include "src/include/execution/executor_factory.h"
#include "src/include/execution/executors/aggregation_executor.h"
#include "src/include/execution/executors/hash_join_executor.h"
#include "src/include/execution/executors/insert_executor.h"
#include "src/include/execution/expressions/aggregate_value_expression.h"
#include "src/include/execution/expressions/column_value_expression.h"
#include "src/include/execution/expressions/comparison_expression.h"
#include "src/include/execution/expressions/constant_value_expression.h"
#include "src/include/execution/plans/seq_scan_plan.h"

#include "/usr/local/include/hsql/SQLParser.h"
#include "/usr/local/include/hsql/util/sqlhelper.h"

using namespace bustub;

class Executor {
 private:
  std::unique_ptr<TransactionManager> txn_mgr_;
  Transaction *txn_{nullptr};
  std::unique_ptr<DiskManager> disk_manager_;
  std::unique_ptr<LogManager> log_manager_ = nullptr;
  std::unique_ptr<LockManager> lock_manager_ = nullptr;
  std::unique_ptr<BufferPoolManager> bpm_;
  std::unique_ptr<SimpleCatalog> catalog_;
  std::unique_ptr<ExecutorContext> exec_ctx_;
  std::vector<std::unique_ptr<AbstractExpression>> allocated_exprs_;
  std::vector<std::unique_ptr<Schema>> allocated_output_schemas_;
  static constexpr uint32_t MAX_VARCHAR_SIZE = 128;

 public:
  void SetUp() {
    // For each test, we create a new DiskManager, BufferPoolManager, TransactionManager, and SimpleCatalog.
    disk_manager_ = std::make_unique<DiskManager>("executor_test.db");
    bpm_ = std::make_unique<BufferPoolManager>(32, disk_manager_.get());
    txn_mgr_ = std::make_unique<TransactionManager>(lock_manager_.get(), log_manager_.get());
    catalog_ = std::make_unique<SimpleCatalog>(bpm_.get(), lock_manager_.get(), log_manager_.get());
    // Begin a new transaction, along with its executor context.
    txn_ = txn_mgr_->Begin();
    exec_ctx_ = std::make_unique<ExecutorContext>(txn_, catalog_.get(), bpm_.get());
    // Generate some test tables.
    TableGenerator gen{exec_ctx_.get()};
    gen.GenerateTestTables();
  }

  ExecutorContext *GetExecutorContext() { return exec_ctx_.get(); }
  const AbstractExpression *MakeColumnValueExpression(const Schema &schema, uint32_t tuple_idx,
                                                      const std::string &col_name) {
    uint32_t col_idx = schema.GetColIdx(col_name);
    auto col_type = schema.GetColumn(col_idx).GetType();
    allocated_exprs_.emplace_back(std::make_unique<ColumnValueExpression>(tuple_idx, col_idx, col_type));
    return allocated_exprs_.back().get();
  }

  const AbstractExpression *MakeConstantValueExpression(const Value &val) {
    allocated_exprs_.emplace_back(std::make_unique<ConstantValueExpression>(val));
    return allocated_exprs_.back().get();
  }

  const AbstractExpression *MakeComparisonExpression(const AbstractExpression *lhs, const AbstractExpression *rhs,
                                                     ComparisonType comp_type) {
    allocated_exprs_.emplace_back(std::make_unique<ComparisonExpression>(lhs, rhs, comp_type));
    return allocated_exprs_.back().get();
  }

  const AbstractExpression *MakeAggregateValueExpression(bool is_group_by_term, uint32_t term_idx) {
    allocated_exprs_.emplace_back(
        std::make_unique<AggregateValueExpression>(is_group_by_term, term_idx, TypeId::INTEGER));
    return allocated_exprs_.back().get();
  }

  const Schema *MakeOutputSchema(const std::vector<std::pair<std::string, const AbstractExpression *>> &exprs) {
    std::vector<Column> cols;
    cols.reserve(exprs.size());
    for (const auto &input : exprs) {
      if (input.second->GetReturnType() != TypeId::VARCHAR) {
        cols.emplace_back(input.first, input.second->GetReturnType(), input.second);
      } else {
        cols.emplace_back(input.first, input.second->GetReturnType(), 128, input.second);
      }
    }
    allocated_output_schemas_.emplace_back(std::make_unique<Schema>(cols));
    return allocated_output_schemas_.back().get();
  }

#define LOG(x) std::cout << #x " = " << x << std::endl;
  void ExecuteSelect(hsql::SelectStatement const *stmt) {
    auto tableName = stmt->fromTable->getName();
    // TableMetadata *table_info = GetExecutorContext()->GetCatalog()->GetTable(tableName);
    TableMetadata *table_info = GetExecutorContext()->GetCatalog()->GetTable (3);
    Schema &schema = table_info->schema_;
    std::vector<AbstractExpression const *> columns;
    std::vector<std::pair<std::string, const AbstractExpression *>> out_schema_els;
    for (auto expr : *(stmt->selectList)) {
      auto *col = MakeColumnValueExpression(schema, 0, expr->getName());
      out_schema_els.push_back({expr->getName(), col});
      columns.push_back(col);
    }
    auto *out_schema = MakeOutputSchema(out_schema_els);
    SeqScanPlanNode plan{out_schema, nullptr, table_info->oid_};
    auto executor = ExecutorFactory::CreateExecutor(GetExecutorContext(), &plan);
    executor->Init();
    Tuple tuple;
    while (executor->Next(&tuple)) {
      for (auto expr : *(stmt->selectList)) {
        std::cout << tuple.GetValue(out_schema, out_schema->GetColIdx(expr->getName())).GetAs<int32_t>() << " ";
      }
      std::cout << std::endl;
    }
  }

  void ExecuteInsert(hsql::InsertStatement const *stmt) {
    txn_ = txn_mgr_->Begin();

    auto const &values = *(stmt->values);
    std::vector<Value> val;
    for (auto expr : values) {
      val.push_back(ValueFactory::GetIntegerValue(expr->ival));
    }
    std::vector<std::vector<Value>> raw_vals{val};

    auto tableName = stmt->tableName;
    // TableMetadata *table_info = GetExecutorContext()->GetCatalog()->GetTable(tableName);
    TableMetadata *table_info = GetExecutorContext()->GetCatalog()->GetTable(3);
    InsertPlanNode insert_plan{std::move(raw_vals), table_info->oid_};
    auto insert_executor = ExecutorFactory::CreateExecutor(GetExecutorContext(), &insert_plan);
    insert_executor->Init();
  insert_executor->Next(nullptr);
    txn_mgr_->Commit(txn_);
    delete txn_;
  }

  ~Executor() {
    bpm_->FlushAllPages();
    disk_manager_->ShutDown();
    std::cout << "destructor called\n";
  }
};