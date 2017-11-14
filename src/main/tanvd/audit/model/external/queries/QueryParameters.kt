package tanvd.audit.model.external.queries

import tanvd.aorm.DbType
import tanvd.aorm.query.Order
import tanvd.aorm.query.OrderByExpression
import tanvd.audit.model.external.types.ColumnWrapper

fun orderBy(vararg pair: Pair<ColumnWrapper<*, DbType<*>>, Order>) : OrderByExpression {
    return OrderByExpression(pair.map { it.first.column to it.second }.toMap())
}
