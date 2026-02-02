#pragma once

#include <Interpreters/Context_fwd.h>

#include <unordered_set>

namespace DB
{

class IQueryTreeNode;
using QueryTreeNodePtr = std::shared_ptr<IQueryTreeNode>;

struct TemporaryTableHolder;
using TemporaryTableHolderPtr = std::shared_ptr<TemporaryTableHolder>;

using ReusedMaterializedCTEs = std::unordered_set<TemporaryTableHolderPtr>;

void inlineMaterializedCTEIfNeeded(QueryTreeNodePtr & node, const ReusedMaterializedCTEs & reused_materialized_cte, ContextPtr context);

}
