package tanvd.audit.implementation.clickhouse

import org.slf4j.LoggerFactory
import tanvd.audit.implementation.clickhouse.AuditDaoClickhouseImpl.Config.descriptionColumn
import tanvd.audit.implementation.clickhouse.model.*
import tanvd.audit.model.AuditType
import java.sql.Connection
import java.sql.ResultSet
import java.sql.SQLException
import java.sql.Statement
import java.util.*
import javax.sql.DataSource

class JdbcClickhouseConnection(val dataSource: DataSource) {

    private val logger = LoggerFactory.getLogger(JdbcClickhouseConnection::class.java)


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
        } catch (e : SQLException) {
            logger.error("Error inside Clickhouse occurred: ", e)
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
        } catch (e: SQLException) {
            logger.error("Error inside Clickhouse occurred: ", e)
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
        sqlInsert.append("INSERT INTO $tableName (${row.toStringHeader()}) VALUES (${row.toStringValues()});")

        var connection : Connection? = null
        var stmt : Statement? = null
        try {
            connection = dataSource.connection
            stmt = connection.createStatement()
            stmt.executeUpdate(sqlInsert.toString())
        } catch (e: SQLException) {
            logger.error("Error inside Clickhouse occurred: ", e)
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
        sqlInsert.append("INSERT INTO $tableName (${tableHeader.toDefString()}) VALUES \n")

        for ((columns) in rows) {
            sqlInsert.append("(")
            for ((name) in tableHeader.columnsHeader) {
                val appropriateColumn = columns.find { it.name == name }
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
        } catch (e: SQLException) {
            logger.error("Error inside Clickhouse occurred: ", e)
        } finally {
            stmt?.close()
            connection?.close()
        }
    }

    /**
     * Loads rows with all specified columns from specified table where column typeName has element id
     */
    fun loadRows(tableName : String, typeName: String, id: String, columnsToSelect : DbTableHeader): List<DbRow> {
        val sqlSelect = "SELECT ${columnsToSelect.toDefString()} FROM $tableName WHERE has($typeName, '$id');"

        val rows = ArrayList<DbRow>()

        var connection : Connection? = null
        var stmt : Statement? = null
        var resultSet : ResultSet? = null
        try {
            connection = dataSource.connection
            stmt = connection.createStatement()
            resultSet = stmt.executeQuery(sqlSelect)

            while (resultSet.next()) {
                val elements = ArrayList<DbColumn>()
                for ((name) in columnsToSelect.columnsHeader) {
                    @Suppress("UNCHECKED_CAST")
                    val resultArray = resultSet.getArray(name).array as Array<String>
                    elements.add(DbColumn(name, resultArray.toList(), DbColumnType.DbArrayString))
                }
                rows.add(DbRow(elements))
            }
        } catch (e: SQLException) {
            logger.error("Error inside Clickhouse occurred: ", e)
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
        } catch (e: SQLException) {
            logger.error("Error inside Clickhouse occurred: ", e)
        } finally {
            stmt?.close()
            connection?.close()
        }
    }
}
