//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/hash_join_executor.h"

namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left, std::unique_ptr<AbstractExecutor> &&right)
    : AbstractExecutor(exec_ctx), plan_(plan), left_(std::move(left)), right_(std::move(right)),
    jht_ ("ephemeral_ht_for_join", exec_ctx->GetBufferPoolManager(), jht_comp_, jht_num_buckets_, jht_hash_fn_)
    {}

/** @return the JHT in use. Do not modify this function, otherwise you will get a zero. */
// const HT *GetJHT() const { return &jht_; }

void HashJoinExecutor::Init() {
    // Build phase
    left_->Init(), right_->Init();
    Tuple tup;
    while (left_->Next(&tup)) {
        hash_t val = HashValues(&tup, GetOutputSchema(), {plan_->Predicate()});
        jht_.Insert (exec_ctx_->GetTransaction(), val, tup);
    }
}

bool HashJoinExecutor::Next(Tuple *tuple) {
    // Probe Phase
    if (processing) {
        std::vector<Tuple> tuples;
        jht_.GetValue(exec_ctx_->GetTransaction(), processing_hash, &tuples);
        for (size_t i = processing_next_index; i < tuples.size(); i++) {
            Tuple & tup = tuples[i];
            if (plan_->Predicate()->EvaluateJoin(&tup, left_->GetOutputSchema(), processing_right_tuple, right_->GetOutputSchema()).GetAs<bool>()) {
                processing_next_index = i + 1;
                return true;
            }
        }
        processing = false;
        return Next(tuple);
    } else {
        std::vector<Tuple> tuples;
        while (right_->Next(tuple)) {
            hash_t hash = HashValues(tuple, GetOutputSchema(), {plan_->Predicate()});
            jht_.GetValue(exec_ctx_->GetTransaction(), hash, &tuples);
            processing = true;
            processing_hash = hash;
            processing_right_tuple = tuple;
            size_t index = 0;
            for (Tuple & tup : tuples) {
                if (plan_->Predicate()->EvaluateJoin(&tup, left_->GetOutputSchema(), tuple, right_->GetOutputSchema()).GetAs<bool>()) {
                    processing_next_index = index + 1;
                    return true;
                }
                index += 1;
            }
            processing = false;
            tuples.clear();
        }
    }
    return false;
    }
}  // namespace bustub
