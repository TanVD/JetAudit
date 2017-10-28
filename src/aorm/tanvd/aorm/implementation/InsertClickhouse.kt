package tanvd.aorm.implementation

import tanvd.aorm.InsertExpression
import java.sql.Connection
import java.sql.PreparedStatement

object InsertClickhouse {
    fun insert(expression: InsertExpression) {
        ConnectionClikhouse.withConnection {
            constructInsert(expression).execute()
        }
    }

    private fun Connection.constructInsert(insert: InsertExpression) : PreparedStatement {
        val sql = "INSERT INTO ${insert.table.name} (${insert.columns.joinToString { it.name }}) VALUES " +
                insert.values.joinToString { "(${insert.columns.joinToString { "?" }})" };
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
}