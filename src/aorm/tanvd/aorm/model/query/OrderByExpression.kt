package tanvd.aorm.model.query

import tanvd.aorm.model.Column

class OrderByExpression(val map: Map<Column<*>, Order>)

enum class Order {
    ASC,
    DESC
}



//helper functions
fun Query.orderBy(vararg orderByMap: Pair<Column<*>, Order>) : Query {
    return this orderBy OrderByExpression(orderByMap.toMap())
}
