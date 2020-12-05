//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

const Schema *InsertExecutor::GetOutputSchema() {
    const auto &catalog = this->GetExecutorContext()->GetCatalog();
  return &catalog->GetTable(this->plan_->TableOid())->schema_;
    // return plan_->OutputSchema(); 
}

void InsertExecutor::Init() {
    SimpleCatalog * const& catalog = GetExecutorContext()->GetCatalog();
    auto table_oid = plan_->TableOid();
    table_ = catalog->GetTable(table_oid)->table_.get();
    if (!plan_->IsRawInsert()) child_executor_->Init();
}

bool InsertExecutor::Next([[maybe_unused]] Tuple *tuple) {
    // Insert everything possible.
    Tuple tup;
    RID rid;
    if (plan_->IsRawInsert()) {
        if (done) {
            return false;
        }
        for (auto & row: plan_->RawValues()) {
            Tuple tp (row, GetOutputSchema());
            if (!table_->InsertTuple(tp, &rid, GetExecutorContext()->GetTransaction())) {
                return false;
            }
        }
        done = true;
        return true;
    } else {
        while (child_executor_->Next(&tup)) {
            if (!table_->InsertTuple(tup,
            &rid,
            GetExecutorContext()->GetTransaction()
            )) {
                return false;
            }
        }
    }
    return true;
}
}  // namespace bustub
