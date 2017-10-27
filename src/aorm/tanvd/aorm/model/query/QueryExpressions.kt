package tanvd.aorm.model.query

import tanvd.aorm.model.Column

sealed class QueryExpression {
    open fun toSqlPreparedDef() : PreparedSqlResult {
        throw NotImplementedError()
    }
}



//Binary logic expressions
sealed class BinaryQueryExpression(val left: QueryExpression, val right: QueryExpression, val op: String) : QueryExpression() {
    override fun toSqlPreparedDef(): PreparedSqlResult {
        val resultLeft = left.toSqlPreparedDef()
        val resultRight = right.toSqlPreparedDef()
        return PreparedSqlResult("(${resultLeft.sql} $op ${resultRight.sql})", resultLeft.data + resultRight.data)
    }
}

class AndQueryExpression(left: QueryExpression, right: QueryExpression) : BinaryQueryExpression(left, right, "AND")
class OrQueryExpression(left: QueryExpression, right: QueryExpression) : BinaryQueryExpression(left, right, "OR")



//Unary logic expressions
sealed class UnaryQueryExpression(val expression: QueryExpression, val op: String) : QueryExpression() {
    override fun toSqlPreparedDef(): PreparedSqlResult {
        val result = expression.toSqlPreparedDef()
        return PreparedSqlResult("($op ${result.sql})", result.data)
    }
}

class NotQueryExpression(expression: QueryExpression): UnaryQueryExpression(expression, "NOT")


sealed class InfixConditionQueryExpression<T : Any>(val column: Column<T>, val value: T, val op: String) : QueryExpression() {
    override fun toSqlPreparedDef(): PreparedSqlResult {
        @Suppress("UNCHECKED_CAST")
        return PreparedSqlResult("(${column.name} $op ?)", listOf((column to value) as Pair<Column<Any>, Any>))
    }
}

//Equality
class EqExpression<T : Any>(column: Column<T>, value: T) : InfixConditionQueryExpression<T>(column, value, "=")

//LessOrMore
class LessExpression<T : Any>(column: Column<T>, value: T) : InfixConditionQueryExpression<T>(column, value, "<")

class LessOrEqualExpression<T : Any>(column: Column<T>, value: T) : InfixConditionQueryExpression<T>(column, value, "<=")

class MoreExpression<T : Any>(column: Column<T>, value: T) : InfixConditionQueryExpression<T>(column, value, ">")

class MoreOrEqualExpression<T : Any>(column: Column<T>, value: T) : InfixConditionQueryExpression<T>(column, value, ">=")

//Like
class LikeExpression(column: Column<String>, value: String) : InfixConditionQueryExpression<String>(column, value, "LIKE")

//To SQL class
data class PreparedSqlResult(val sql: String, val data: List<Pair<Column<Any>, Any>>)


//Helper functions
//logic operators
infix fun QueryExpression.and(value: QueryExpression): AndQueryExpression {
    return AndQueryExpression(this, value)
}

infix fun QueryExpression.or(value: QueryExpression): OrQueryExpression {
    return OrQueryExpression(this, value)
}

fun not(value: QueryExpression): NotQueryExpression {
    return NotQueryExpression(value)
}

//Equality
infix fun <T: Any>Column<T>.eq(value: T) : EqExpression<T> {
    return EqExpression(this, value)
}

//Numbers
infix fun <T: Number>Column<T>.less(value: T) : LessExpression<T> {
    return LessExpression(this, value)
}

infix fun <T: Number>Column<T>.lessOrEqual(value: T) : LessOrEqualExpression<T> {
    return LessOrEqualExpression(this, value)
}

infix fun <T: Number>Column<T>.more(value: T) : MoreExpression<T> {
    return MoreExpression(this, value)
}

infix fun <T: Number>Column<T>.moreOrEqual(value: T) : MoreOrEqualExpression<T> {
    return MoreOrEqualExpression(this, value)
}

//Strings
infix fun Column<String>.like(value: String): LikeExpression {
    return LikeExpression(this, value)
}

