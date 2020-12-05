//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan) :
AbstractExecutor(exec_ctx), plan_(plan) {}

// Called before calls to Next()
// Set up metadata
void SeqScanExecutor::Init() {
    SimpleCatalog * const& catalog = GetExecutorContext()->GetCatalog();
    auto table_oid = plan_->GetTableOid();
    table_ = catalog->GetTable(table_oid)->table_.get();
    iter_ = std::make_unique<TableIterator> (
        table_->Begin(GetExecutorContext()->GetTransaction())
    );
}

bool SeqScanExecutor::Next(Tuple *tuple) {
    auto & iter = *iter_;
    while (*iter_ != table_->End()) {
        auto next_tuple = *(iter++);
        auto predicate = plan_->GetPredicate();
        if (predicate != nullptr) {
            bool cond = predicate->Evaluate(&next_tuple, GetOutputSchema()).GetAs<bool> ();
            if (cond) {
                *tuple = Tuple(next_tuple);
                return true;
            }
        } else { // No predicate on SELECT
            *tuple = Tuple (next_tuple);
            return true;
        }
    }
    return false;
}
}  // namespace bustub
