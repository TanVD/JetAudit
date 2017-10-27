package tanvd.aorm.model.query

import tanvd.aorm.model.Column
import tanvd.aorm.model.Row
import tanvd.aorm.model.Table
import tanvd.aorm.model.implementation.QueryClickhouse

class Query(val table: Table, val columns: List<Column<Any>>) {
    internal var whereSection : QueryExpression? = null
    internal var prewhereSection: QueryExpression? = null
    internal var orderBySection: OrderByExpression? = null
    internal var limitSection: LimitExpression? = null

    fun toResult(): List<Row> {
        return QueryClickhouse.getResult(this)
    }
}



//Helper functions
infix fun Query.where(expression: QueryExpression) : Query {
    whereSection = expression
    return this
}

infix fun Query.prewhere(expression: QueryExpression) : Query {
    prewhereSection = expression
    return this
}

infix fun Query.orderBy(expression: OrderByExpression) : Query {
    orderBySection = expression
    return this
}

infix fun Query.limit(expression: LimitExpression): Query {
    limitSection = expression
    return this
}