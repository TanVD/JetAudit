package tanvd.audit.model.external.queries

/**
 * Generic interface for all query expressions to JetAudit.
 */
interface QueryExpression

/**
 * Operators for expression to construct tree.
 */
enum class BinaryQueryOperator {
    and,
    or
}

enum class UnaryQueryOperator {
    not
}

/**
 * Binary operators of an expression tree.
 */
class BinaryQueryNode(val binaryQueryOperator: BinaryQueryOperator, val expressionFirst: QueryExpression,
                      val expressionSecond: QueryExpression) : QueryExpression


infix fun QueryExpression.and(expression: QueryExpression): QueryExpression {
    return BinaryQueryNode(BinaryQueryOperator.and, this, expression)
}

infix fun QueryExpression.or(expression: QueryExpression): QueryExpression {
    return BinaryQueryNode(BinaryQueryOperator.or, this, expression)
}

/**
 * Unary operators of an expression tree
 */

class UnaryQueryNode(val unaryQueryOperator: UnaryQueryOperator, val expression: QueryExpression) : QueryExpression

fun not(expression: QueryExpression): QueryExpression {
    return UnaryQueryNode(UnaryQueryOperator.not, expression)
}



