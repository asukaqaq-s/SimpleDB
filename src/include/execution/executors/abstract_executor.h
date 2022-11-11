#ifndef ABSTRACT_EXECUTOR_H
#define ABSTRACT_EXECUTOR_H

#include "execution/plans/abstract_plan.h"
#include "concurrency/transaction.h"
#include "execution/executor_context.h"

namespace SimpleDB {

class AbstractExecutor {

public:

    /**
    * @brief Construct a new Abstract Executor object
    * @param context 
    * @param node 
    */
    AbstractExecutor(ExecutionContext *content, AbstractPlan *node)
        : content_(content), node_(node) {}
    
    
    virtual ~AbstractExecutor() = default;


    /**
    * @brief 
    * Initialize this executor
    */
    virtual void Init() = 0;

    
    /**
    * @brief 
    * Get a tuple from child executor
    * @param[out] tuple tuple from child executor
    * @return true when succeed, false when there are no more tuples
    */
    virtual bool Next(Tuple *tuple) = 0;


    /**
    * @brief 
    * convenience method to return plan node corresponding to this executor.
    *
    * @tparam T 
    * @return const T& 
    */
    template <class T>
    inline const T &GetPlanNode() {
        const T *node = dynamic_cast<const T *>(node_);
        SIMPLEDB_ASSERT(node != nullptr, "invalid casting");
        return *node;
    }


    Schema GetOutputSchema() {
        return *node_->GetSchema();
    }



protected:

    // execution context
    ExecutionContext *content_;
    // plan node corresponding to this executor
    AbstractPlan *node_;
    
};

} // namespace SimpleDB

#endif