#ifndef EXECUTION_ENGINE_H
#define EXECUTION_ENGINE_H

#include "record/schema.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"
#include "config/exception.h"
#include "plans/abstract_plan.h"
#include "execution/executor_factory.h"
#include "execution/executor_context.h"

namespace SimpleDB {

/**
* @brief 
* ExecutionEngine is used to perform the execution based on PlanTree
*/
class ExecutionEngine {

public:

    ExecutionEngine() = default;

    void Execute(ExecutionContext *content, AbstractPlan *plan, std::vector<Tuple> *result_set) {
        auto executor = ExecutorFactory::CreateExecutor(content, plan);

        // initialize executor
        executor->Init();

        // execute
        try {
            Tuple tuple;
            while (executor->Next(&tuple)) {
                if (result_set != nullptr) {
                    result_set->push_back(tuple);
                }
            }
        } catch (TransactionAbortException &e) {
            // LOG_INFO("%s", e.reason_.c_str());
            content->GetTransactionManager()->Abort(content->GetTransactionContext());
        }

    }

};

}

#endif






