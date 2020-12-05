//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx), plan_(plan), child_(std::move (child)),
    aht_(plan->GetAggregates(), plan->GetAggregateTypes()),
    aht_iterator_ (aht_.Begin())
     {}

const AbstractExecutor *AggregationExecutor::GetChildExecutor() const { return child_.get(); }

const Schema *AggregationExecutor::GetOutputSchema() { return plan_->OutputSchema(); }

void AggregationExecutor::Init() {
    child_->Init();
    Tuple tup;
    while (child_->Next(&tup)) {
        AggregateKey key = MakeKey (&tup);
        AggregateValue val = MakeVal (&tup);
        aht_.InsertCombine(key, val);
    }
}

bool AggregationExecutor::Next(Tuple *tuple) {
    while (aht_iterator_ != aht_.End()) {
        
    }
    return false; }

}  // namespace bustub
