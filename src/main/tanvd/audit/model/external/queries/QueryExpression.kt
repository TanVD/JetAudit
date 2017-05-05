package tanvd.audit.model.external.queries

/**
 * Generic interface for all query expressions to JetAudit.
 */
interface QueryExpression

/**
 * Operators for expression to construct tree.
 */
enum class QueryOperator {
    and,
    or
}

/**
 * Node of an expression tree.
 */
class QueryNode(val queryOperator: QueryOperator, val expressionFirst: QueryExpression,
                val expressionSecond: QueryExpression) : QueryExpression


infix fun QueryExpression.and(expression: QueryExpression): QueryExpression {
    return QueryNode(QueryOperator.and, this, expression)
}

infix fun QueryExpression.or(expression: QueryExpression): QueryExpression {
    return QueryNode(QueryOperator.or, this, expression)
}

