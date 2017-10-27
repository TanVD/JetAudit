package tanvd.aorm.model.implementation

import tanvd.aorm.model.InsertExpression
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
            for ((values) in insert.values) {
                for (column in insert.columns) {
                    if (values[column] != null) {
                        column.setValue(index, it, values[column]!!)
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