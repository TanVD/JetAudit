package tanvd.audit.model.external

import tanvd.audit.exceptions.UnknownAuditTypeException
import kotlin.reflect.KClass

/**
 * In Clickhouse you can use one type multiply times in one expression.
 *
 * In MySQL you MUST NOT use one type multiply times in one expression.
 */

/** Generic interface for all query expressions to Audit. **/
interface QueryExpression

/** Node of an expression tree. */
class QueryNode(val queryOperator: QueryOperator, val expressionFirst: QueryExpression,
                val expressionSecond: QueryExpression) : QueryExpression

/** Operators for expression to construct tree. **/
enum class QueryOperator {
    and,
    or
}

/** Leaf with AuditType. **/
class QueryTypeLeaf(val typeCondition: QueryTypeCondition, val id: String, klass: KClass<*>) : QueryExpression {
    val type: AuditType<Any> = AuditType.resolveType(klass)
}

/** Operators permitted to use with AuditTypes. **/
enum class QueryTypeCondition {
    equal,
    like
}

/** Class for TimeStamp to use in expression's declarations. **/
class AuditUnixTimeStamp

/** Leaf with UnixTimeStamp. **/
class QueryTimeStampLeaf(val condition: QueryTimeStampCondition, val timeStamp: Int) : QueryExpression

/** Operators permitted to use with UnixTimeStamp. **/
enum class QueryTimeStampCondition {
    less,
    more,
    equal
}


/** Infix functions to construct readable expressions. **/

@Throws(UnknownAuditTypeException::class)
infix fun KClass<*>.equal(obj: Any): QueryTypeLeaf {
    return QueryTypeLeaf(QueryTypeCondition.equal, AuditType.TypesResolution.resolveType(this).serialize(obj), this)
}

@Throws(UnknownAuditTypeException::class)
infix fun KClass<*>.like(obj: String): QueryTypeLeaf {
    return QueryTypeLeaf(QueryTypeCondition.like, AuditType.TypesResolution.resolveType(this).serialize(obj), this)
}


infix fun QueryExpression.and(expression: QueryExpression): QueryExpression {
    return QueryNode(QueryOperator.and, this, expression)
}

infix fun QueryExpression.or(expression: QueryExpression): QueryExpression {
    return QueryNode(QueryOperator.or, this, expression)
}

infix fun KClass<AuditUnixTimeStamp>.lessThan(number: Int): QueryTimeStampLeaf {
    return QueryTimeStampLeaf(QueryTimeStampCondition.less, number)
}

infix fun KClass<AuditUnixTimeStamp>.moreThan(number: Int): QueryTimeStampLeaf {
    return QueryTimeStampLeaf(QueryTimeStampCondition.more, number)
}

infix fun KClass<AuditUnixTimeStamp>.equalTo(number: Int): QueryTimeStampLeaf {
    return QueryTimeStampLeaf(QueryTimeStampCondition.equal, number)
}

