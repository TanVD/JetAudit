package tanvd.audit.implementation.clickhouse

import ru.yandex.clickhouse.except.ClickHouseUnknownException
import tanvd.audit.implementation.clickhouse.AuditDaoClickhouseImpl.Config.auditDescriptionColumnName
import tanvd.audit.implementation.clickhouse.model.*
import tanvd.audit.model.AuditType
import java.sql.Connection
import java.sql.ResultSet
import java.sql.Statement
import java.util.*
import javax.sql.DataSource

class JdbcClickhouseConnection(val dataSource: DataSource) {
    /**
     * Creates table with specified header (uses ifNotExists modifier by default)
     * You must specify date field cause we use MergeTree Engine
     */
    fun createTable(tableName: String, tableHeader: DbTableHeader,
                    primaryKey: String, dateKey: String,
                    setDefaultDate : Boolean = true, ifNotExists: Boolean = true) {
        val sqlCreate = StringBuilder()
        sqlCreate.append("CREATE TABLE ")
        sqlCreate.append(if (ifNotExists) "IF NOT EXISTS " else "")
        sqlCreate.append(tableHeader.columnsHeader.joinToString(prefix = "$tableName (", postfix = ") ") {
            if (it.name == dateKey && setDefaultDate)
                "${it.name} ${it.type} DEFAULT today()"
            else
                "${it.name} ${it.type}"
        })
        sqlCreate.append("ENGINE = MergeTree($dateKey, ($primaryKey), 8192);")

        var connection : Connection? = null
        var stmt : Statement? = null
        try {
            connection = dataSource.connection
            stmt = connection.createStatement()
            stmt.execute(sqlCreate.toString())
        } finally {
            stmt?.close()
            connection?.close()
        }
    }

    /**
     * Adds column in a table with specified name and name
     */
    fun addColumn(tableName: String, columnHeader : DbColumnHeader) {
        val sqlAlter = "ALTER TABLE $tableName ADD COLUMN ${columnHeader.name} ${columnHeader.type} ;"

        var connection : Connection? = null
        var stmt : Statement? = null
        try {
            connection = dataSource.connection
            stmt = connection.createStatement()
            stmt.execute(sqlAlter)
        } catch (e: ClickHouseUnknownException) {
            println("Column ${columnHeader.name} already exists.")
        } finally {
            stmt?.close()
            connection?.close()
        }
    }

    /**
     * Insert only in columns which row contains
     */
    fun insertRow(tableName: String, row: DbRow) {
        val sqlInsert = StringBuilder()
        sqlInsert.append("INSERT INTO $tableName (${row.columns.map { it.name }.joinToString()}) VALUES ")

        sqlInsert.append(row.toStringInsert())

        sqlInsert.append(";")

        var connection : Connection? = null
        var stmt : Statement? = null
        try {
            connection = dataSource.connection
            stmt = connection.createStatement()
            stmt.executeUpdate(sqlInsert.toString())
        } finally {
            stmt?.close()
            connection?.close()
        }
    }

    /**
     * Insert in all columns (Some maybe not stated).
     * (Better not to seek for intersection of columns of all rows)
     */
    fun insertRows(tableName: String, tableHeader: DbTableHeader, rows: List<DbRow>) {
        val sqlInsert = StringBuilder()
        sqlInsert.append("INSERT INTO $tableName ${tableHeader.toStringInsert()} VALUES \n")

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

        var connection : Connection? = null
        var stmt : Statement? = null
        try{
            connection = dataSource.connection
            stmt = connection.createStatement()
            stmt.executeUpdate(sqlInsert.toString())
        } finally {
            stmt?.close()
            connection?.close()
        }
    }

    /**
     * Loads all audits connected to this object
     * Returns list of records, every record is list of pairs <name, ids>
     * always description will be included
     */
    fun loadRows(tableName : String, typeName: String, id: String): List<DbRow> {
        val sqlSelect = "SELECT * FROM $tableName WHERE has($typeName, '$id');"

        val rows = ArrayList<DbRow>()

        var connection : Connection? = null
        var stmt : Statement? = null
        var resultSet : ResultSet? = null
        try {
            connection = dataSource.connection
            stmt = connection.createStatement()
            resultSet = stmt.executeQuery(sqlSelect)

            while (resultSet.next()) {
                val row = DbRow()
                @Suppress("UNCHECKED_CAST")
                row.columns.add(
                        DbColumn(auditDescriptionColumnName, (resultSet.getArray("description").array as Array<String>)
                                .toList(), DbColumnType.DbArrayString))
                for (type in AuditType.getTypes()) {
                    @Suppress("UNCHECKED_CAST")
                    val resultArray = resultSet.getArray(type.code).array as Array<String>
                    row.columns.add(DbColumn(type.code, resultArray.toList(), DbColumnType.DbArrayString))
                }
                rows.add(row)
            }
        } finally {
            resultSet?.close()
            stmt?.close()
            connection?.close()
        }


        return rows
    }

    fun dropTable(tableName: String, ifExists: Boolean) {
        val sqlDrop = "DROP TABLE ${if (ifExists) "IF EXISTS" else ""} $tableName;"

        var connection : Connection? = null
        var stmt : Statement? = null
        try {
            connection = dataSource.connection
            stmt = connection.createStatement()
            stmt.executeUpdate(sqlDrop)
        } finally {
            stmt?.close()
            connection?.close()
        }
    }




}
