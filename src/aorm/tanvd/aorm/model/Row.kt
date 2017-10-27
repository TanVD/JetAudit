package tanvd.aorm.model

import java.sql.ResultSet

data class Row(private val values: Map<Column<Any>, Any>) {

    constructor(result: ResultSet, columns: List<Column<Any>>) : this(columns.map { it to it.getValue(result) }.toMap())

    operator fun get(column: Column<Any>) : Any? {
        return values[column]
    }
}