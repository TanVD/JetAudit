package tanvd.aorm.implementation

import tanvd.aorm.InsertExpression
import tanvd.aorm.Table
import java.sql.Connection
import java.sql.PreparedStatement

object InsertClickhouse {
    fun insert(table: Table, expression: InsertExpression) {
        table.db.withConnection {
            constructInsert(expression).execute()
        }
    }

    private fun Connection.constructInsert(insert: InsertExpression) : PreparedStatement {
        val sql = "INSERT INTO ${insert.table.name} (${insert.columns.joinToString { it.name }}) VALUES " +
                insert.values.joinToString { "(${insert.columns.joinToString { "?" }})" }
        return prepareStatement(sql).use {
            var index = 1
            for (row in insert.values) {
                for (column in insert.columns) {
                    if (row[column] != null) {
                        column.setValue(index, it, row[column]!!)
                    } else {
                        column.setValue(index, it, column.defaultFunction!!())
                    }
                    index++
                }
            }
            it
        }
    }

    fun constructInsert(insert: InsertExpression): String {
        return  "INSERT INTO ${insert.table.name} (${insert.columns.joinToString { it.name }}) VALUES " +
                "${insert.values.joinToString(prefix = "(", postfix = ")") { row ->
                    insert.columns.joinToString {
                        if (row[it] != null) {
                            it.toStringValue(row[it]!!)
                        } else {
                            it.toStringValue(it.defaultFunction!!())
                        }
                    }
                }};"
    }
}