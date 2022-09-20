package tanvd.jetaudit.model.external

import tanvd.aorm.DbType
import tanvd.aorm.query.*
import tanvd.jetaudit.model.external.types.ColumnWrapper
import tanvd.jetaudit.model.external.types.information.InformationType
import tanvd.jetaudit.model.external.types.objects.StateType

//INFORMATION QUERIES
infix fun <T : Any> InformationType<T>.equal(value: T): QueryExpression = column eq value

//String interface:
infix fun InformationType<String>.like(value: String): QueryExpression = column like value

infix fun InformationType<String>.iLike(value: String): QueryExpression = column iLike value

infix fun InformationType<String>.regex(value: String): QueryExpression = column regex value

//Less or eq interface
infix fun <T : Any> InformationType<T>.less(value: T): QueryExpression = column less value

infix fun <T : Any> InformationType<T>.lessOrEq(value: T): QueryExpression = column lessOrEq value

infix fun <T : Any> InformationType<T>.more(value: T): QueryExpression = column more value

infix fun <T : Any> InformationType<T>.moreOrEq(value: T): QueryExpression = column moreOrEq value

//List interface
infix fun <T : Any> InformationType<T>.inList(values: List<T>): QueryExpression = column inList values


//TYPE QUERIES

infix fun <T : Any> StateType<T>.equal(number: T): QueryExpression = column exists { x -> x eq number }

//String interface
infix fun StateType<String>.like(value: String): QueryExpression = column exists { x -> x like value }

infix fun StateType<String>.iLike(value: String): QueryExpression = column exists { x -> x iLike value }

infix fun StateType<String>.regex(pattern: String): QueryExpression = column exists { x -> x regex pattern }

//Number interface
infix fun <T : Number> StateType<T>.less(value: T): QueryExpression = column exists { x -> x less value }

infix fun <T : Number> StateType<T>.lessOrEq(value: T): QueryExpression = column exists { x -> x lessOrEq value }

infix fun <T : Number> StateType<T>.more(value: T): QueryExpression = column exists { x -> x more value }

infix fun <T : Number> StateType<T>.moreOrEq(value: T): QueryExpression = column exists { x -> x moreOrEq value }

//List interface
infix fun <T : Any> StateType<T>.inList(value: List<T>): QueryExpression = column exists { x -> x inList value }

//QUERY PARAMETERS
fun orderBy(vararg pair: Pair<ColumnWrapper<*, DbType<*>>, Order>): OrderByExpression =
        OrderByExpression(pair.associate { it.first.column to it.second })

fun limit(limit: Long, offset: Long): LimitExpression = LimitExpression(limit, offset)