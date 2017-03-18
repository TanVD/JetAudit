package tanvd.audit.implementation.clickhouse

import ru.yandex.clickhouse.except.ClickHouseUnknownException
import tanvd.audit.implementation.clickhouse.model.*
import java.sql.Connection
import java.util.*

class JdbcClickhouseConnection(val connection: Connection) {
    /**
     * Creates table with specified header (uses ifNotExists modifier by default)
     * You must specify date field
     * We use MergeTree engine
     */
    fun createTable(tableName: String, tableHeader: DbTableHeader,
                    primaryKey: String, dateKey: String,
                    setDefaultDate : Boolean = true, ifNotExists: Boolean = true) {
        val sqlCreate = StringBuilder()
        sqlCreate.append("CREATE TABLE ")
        sqlCreate.append(if (ifNotExists) "IF NOT EXISTS " else "")
        sqlCreate.append(tableHeader.columnsHeader.joinToString(prefix = "$tableName (", postfix = ") ") {
            if (it.name == dateKey && setDefaultDate) "${it.name} ${it.type} DEFAULT today()"
                else "${it.name} ${it.type}" })
        sqlCreate.append("ENGINE = MergeTree($dateKey, ($primaryKey), 8192);")

        val stmt = connection.createStatement()
        stmt.execute(sqlCreate.toString())
    }

    /**
     * Adds column in a table with specified name and name
     */
    fun addColumn(tableName: String, columnHeader : DbColumnHeader) {
        val sqlAlter = "ALTER TABLE $tableName ADD COLUMN ${columnHeader.name} ${columnHeader.type} ;"

        val stmt = connection.createStatement()
        try {
            stmt.execute(sqlAlter)
        } catch (e: ClickHouseUnknownException) {
            println("Column ${columnHeader.name} already exists.")
        }
    }

    private fun DbRow.prepareForDb() : String {
        return this.columns.joinToString(prefix = "(", postfix = ")") {
            column ->
            when (column.type) {
                DbColumnType.DbDate -> {
                    column.elements[0]
                }
                DbColumnType.DbString -> {
                    "'${column.elements[0]}'"
                }
                DbColumnType.DbArrayString -> {
                    "[${column.elements.joinToString { "'$it'" }}]"
                }
            }
        }
    }

    /**
     * Insert only in columns which row contains
     */
    fun insertRow(tableName: String, row: DbRow) {
        val sqlInsert = StringBuilder()
        sqlInsert.append("INSERT INTO $tableName (${row.columns.map { it.name }.joinToString()}) VALUES ")

        sqlInsert.append(row.prepareForDb())

        sqlInsert.append(";")

        val stmt = connection.createStatement()
        stmt.executeUpdate(sqlInsert.toString())
    }

    /**
     * Insert in all columns (Some maybe not stated).
     * (Better not to seek for intersection of columns of all rows)
     */
    fun insertRows(tableName: String, tableHeader: DbTableHeader, rows: List<DbRow>) {
        val sqlInsert = StringBuilder()
        sqlInsert.append("INSERT INTO $tableName (${tableHeader.columnsHeader.joinToString {it.name}}) VALUES \n")

        for ((columns) in rows) {
            sqlInsert.append("(")
            for (column in tableHeader.columnsHeader) {
                val appropriateColumn = columns.find { it.name == column.name }
                if (appropriateColumn != null) {
                    when (appropriateColumn.type) {
                        DbColumnType.DbDate -> {
                            sqlInsert.append("${appropriateColumn.elements[0]},")
                        }
                        DbColumnType.DbString -> {
                            sqlInsert.append("'${appropriateColumn.elements[0]}',")
                        }
                        DbColumnType.DbArrayString -> {
                            sqlInsert.append("[${appropriateColumn.elements.joinToString { "'$it'" }}],")
                        }
                    }
                } else {
                    sqlInsert.append("[],")
                }
            }
            if (sqlInsert.last() == ',') {
                sqlInsert.deleteCharAt(sqlInsert.length - 1)
            }
            sqlInsert.append("),\n")
        }

        if (sqlInsert.last() == '\n') {
            sqlInsert.deleteCharAt(sqlInsert.length - 1)
            sqlInsert.deleteCharAt(sqlInsert.length - 1)
        }
        sqlInsert.append(";")

        val stmt = connection.createStatement()
        stmt.executeUpdate(sqlInsert.toString())
    }

    /**
     * Loads all audits connected to this object
     * Returns list of records, every record is list of pairs <name, ids>
     * always description will be included
     */
    fun loadRows(tableName : String, typeName: String, id: String): List<DbRow> {
        val sqlSelect = "SELECT * FROM $tableName WHERE has($typeName, '$id');"
        val stmt = connection.createStatement()
        val result = stmt.executeQuery(sqlSelect)

        val selectedTable = ArrayList<DbRow>()
        while (result.next()) {
            val row = DbRow()
            row.columns.add(DbColumn("description", listOf(result.getString("description")), DbColumnType.DbString))
            for (type in AuditDaoClickhouseImpl.types) {
                @Suppress("UNCHECKED_CAST")
                val resultArray = result.getArray(type.code).array as Array<String>
                row.columns.add(DbColumn(type.code, resultArray.toList(), DbColumnType.DbArrayString))
            }
            selectedTable.add(row)
        }

        return selectedTable
    }

    fun dropTable(tableName: String, ifExists: Boolean) {
        val sqlDrop = "DROP TABLE ${if (ifExists) "IF EXISTS" else ""} $tableName;"

        val stmt = connection.createStatement()
        stmt.executeUpdate(sqlDrop)
    }
}
