package tanvd.aorm.implementation

import ru.yandex.clickhouse.ClickHouseConnection
import ru.yandex.clickhouse.ClickHouseDatabaseMetadata
import tanvd.aorm.Table

object MetadataClickhouse {
    fun syncScheme(table: Table) {
        val existsTable = table.db.withConnection {
            val metadata = ClickHouseDatabaseMetadata(table.db.url, this as ClickHouseConnection)
            metadata.getTables(null, table.db.name, table.name, null).use {
                it.next()
            }
        }
        if (!existsTable) {
            TableClickhouse.create(table)
            return
        }
        val dbTableColumns = table.db.withConnection {
            val metadata = ClickHouseDatabaseMetadata(table.db.url, this as ClickHouseConnection)
            metadata.getColumns(null, table.db.name, table.name, null).use {
                val tableColumns = ArrayList<Pair<String, String>>()
                while (it.next()) {
                    tableColumns.add(it.getString("COLUMN_NAME") to it.getString("TYPE_NAME"))
                }
                tableColumns
            }
        }

        for (column in table.columns) {
            val exists = dbTableColumns.any { (name, type) -> name.equals(column.name, true)
                    && type.equals(column.type.toSqlName(), true)}
            if (!exists) {
                TableClickhouse.addColumn(table, column)
            }
        }
    }
}