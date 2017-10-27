package tanvd.aorm.model.implementation

import tanvd.aorm.model.Column
import tanvd.aorm.model.Table

object TableClickhouse {
    fun create(table: Table) {
        ConnectionClikhouse.execute("CREATE TABLE ${table.name} " +
                "(${table.columns.joinToString { it.toSqlDef() }}) " +
                "ENGINE = ${table.engine.toSqlDef()};")
    }

    fun drop(table: Table) {
        ConnectionClikhouse.execute("DROP TABLE ${table.name};")
    }

    fun addColumn(table: Table, column: Column<*>) {
        ConnectionClikhouse.execute("ALTER TABLE ${table.name} ADD COLUMN ${column.toSqlDef()};")
    }

    fun dropColumn(table: Table, column: Column<*>) {
        ConnectionClikhouse.execute("ALTER TABLE ${table.name} DROP COLUMN ${column.name};")
    }
}