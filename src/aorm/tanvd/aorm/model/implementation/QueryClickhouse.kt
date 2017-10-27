package tanvd.aorm.model.implementation

import tanvd.aorm.model.Column
import tanvd.aorm.model.Row
import tanvd.aorm.model.query.Query
import java.sql.Connection
import java.sql.PreparedStatement

object QueryClickhouse {
    fun getResult(query: Query) : List<Row> {
        val rows = ArrayList<Row>()
        ConnectionClikhouse.withConnection {
            constructQuery(query).use { statement ->
                val result = statement.executeQuery()
                while (result.next()) {
                    rows.add(Row(result, query.columns))
                }
            }
        }
        return rows
    }

    private fun Connection.constructQuery(query: Query) : PreparedStatement {
        var sql = "SELECT ${query.columns.joinToString { it.name }} FROM ${query.table.name} "
        val valuesToSet = ArrayList<Pair<Column<Any>, Any>>()
        if (query.prewhereSection != null) {
            val result = query.prewhereSection!!.toSqlPreparedDef()
            sql += "PREWHERE ${result.sql} "
            valuesToSet += result.data
        }
        if (query.whereSection != null) {
            val result = query.whereSection!!.toSqlPreparedDef()
            sql += "WHERE ${result.sql} "
            valuesToSet += result.data
        }
        if (query.orderBySection != null) {
            sql += "ORDER BY ${query.orderBySection!!.map.toList().joinToString { "${it.first.name} ${it.second}" }} "
        }
        if (query.limitSection != null) {
            sql += "LIMIT ${query.limitSection!!.offset} ${query.limitSection!!.limit} "
        }
        sql += ";"
        return prepare(sql, valuesToSet)
    }

    private fun Connection.prepare(sql: String, values: List<Pair<Column<Any>, Any>>) : PreparedStatement {
        val statement = prepareStatement(sql)
        for ((index, pair) in values.withIndex()) {
            val column = pair.first
            val value = pair.second
            column.setValue(index + 1, statement, value)
        }
        return statement
    }
}