package tanvd.audit.implementation.mysql

import org.slf4j.LoggerFactory
import tanvd.audit.implementation.mysql.AuditDaoMysqlImpl.Scheme.auditIdColumn
import tanvd.audit.implementation.mysql.AuditDaoMysqlImpl.Scheme.auditIdInTypeTable
import tanvd.audit.implementation.mysql.AuditDaoMysqlImpl.Scheme.auditTable
import tanvd.audit.implementation.mysql.AuditDaoMysqlImpl.Scheme.descriptionColumn
import tanvd.audit.implementation.mysql.AuditDaoMysqlImpl.Scheme.getPredefinedAuditTableColumn
import tanvd.audit.implementation.mysql.AuditDaoMysqlImpl.Scheme.typeIdColumn
import tanvd.audit.implementation.mysql.AuditDaoMysqlImpl.Scheme.unixTimeStampColumn
import tanvd.audit.implementation.mysql.model.DbColumn
import tanvd.audit.implementation.mysql.model.DbColumnType
import tanvd.audit.implementation.mysql.model.DbRow
import tanvd.audit.implementation.mysql.model.DbTableHeader
import java.sql.*
import java.util.*
import javax.sql.DataSource


/**
 * Provides simple DSL for JDBC clickhouseConnection
 */
class JdbcMysqlConnection(val dataSource: DataSource) {

    private val logger = LoggerFactory.getLogger(JdbcMysqlConnection::class.java)

    /**
     * Creates table with specified header (uses ifNotExists modifier by default)
     */
    fun createTable(tableName: String, tableHeader: DbTableHeader, primaryKey: String,
                    autoIncrement: Boolean, ifNotExists: Boolean = true) {
        val sqlCreate = StringBuilder()
        sqlCreate.append("CREATE TABLE ${if (ifNotExists) "IF NOT EXISTS " else ""} $tableName (")
        for (columnHeader in tableHeader.columnsHeader) {
            sqlCreate.append(columnHeader.toDefString())
            if (columnHeader.name == primaryKey) {
                sqlCreate.append(" NOT NULL ")
                if (autoIncrement) {
                    sqlCreate.append("AUTO_INCREMENT ")
                }
            }
            sqlCreate.append(",")
        }
        sqlCreate.append("PRIMARY KEY ($primaryKey));")

        var connection: Connection? = null
        var preparedStatement: PreparedStatement? = null
        try {
            connection = dataSource.connection
            preparedStatement = connection.prepareStatement(sqlCreate.toString())
            preparedStatement.execute()
        } catch (e: Throwable) {
            logger.error("Error inside MySQL occurred: ", e)
        } finally {
            preparedStatement?.close()
            connection?.close()
        }
    }

    /**
     * Inserts row in MySQL. All values will be sanitized by MySQL JDBC driver
     */
    fun insertRow(tableName: String, row: DbRow): Int {
        val sqlInsert = StringBuilder()
        sqlInsert.append("INSERT INTO $tableName (${row.toStringHeader()}) VALUES (${row.toPlaceholders()});")

        var connection: Connection? = null
        var preparedStatement: PreparedStatement? = null
        var resultSet: ResultSet? = null
        var id = -1
        try {
            connection = dataSource.connection
            preparedStatement = connection.prepareStatement(sqlInsert.toString(), Statement.RETURN_GENERATED_KEYS)

            for ((index, column) in row.columns.withIndex()) {
                preparedStatement.setColumn(column, index + 1)
            }

            preparedStatement.executeUpdate()
            resultSet = preparedStatement.generatedKeys

            if (resultSet.next()) {
                id = resultSet.getInt(1)
            }
        } catch (e: Throwable) {
            logger.error("Error inside MySQL occurred: ", e)
        } finally {
            resultSet?.close()
            preparedStatement?.close()
            connection?.close()
        }

        return id
    }

    /**
     * Loads all rows connected to object with specified id
     * Id will be sanitized by MySQL JDBC driver
     */
    fun loadRows(typeName: String, id: String): List<DbRow> {
        val sqlSelect = "SELECT $descriptionColumn, $unixTimeStampColumn FROM $auditTable INNER JOIN $typeName ON " +
                "$typeName.$auditIdInTypeTable = $auditTable.$auditIdColumn WHERE $typeName.$typeIdColumn = ?;"

        val rows = ArrayList<DbRow>()

        var connection: Connection? = null
        var preparedStatement: PreparedStatement? = null
        var resultSet: ResultSet? = null
        try {
            connection = dataSource.connection
            preparedStatement = connection.prepareStatement(sqlSelect)
            preparedStatement.setString(1, id)
            resultSet = preparedStatement.executeQuery()
            while (resultSet.next()) {
                val description = resultSet.getString(descriptionColumn)
                val timeStamp = resultSet.getInt(unixTimeStampColumn)
                rows.add(DbRow(DbColumn(getPredefinedAuditTableColumn(descriptionColumn), description),
                               DbColumn(getPredefinedAuditTableColumn(unixTimeStampColumn), timeStamp.toString())))
            }
        } catch (e: Throwable) {
            logger.error("Error inside MySQL occurred: ", e)
        } finally {
            resultSet?.close()
            preparedStatement?.close()
            connection?.close()
        }
        return rows
    }

    fun dropTable(tableName: String, ifExists: Boolean) {
        val sqlDrop = "DROP TABLE ${if (ifExists) "IF EXISTS" else ""} $tableName;"

        var connection: Connection? = null
        var stmt: Statement? = null
        try {
            connection = dataSource.connection
            stmt = connection.createStatement()
            stmt.executeUpdate(sqlDrop)
        } catch (e: Throwable) {
            logger.error("Error inside MySQL occurred: ", e)
        } finally {
            stmt?.close()
            connection?.close()
        }
    }

    private fun PreparedStatement.setColumn(column : DbColumn, dbIndex: Int) {
        when (column.type) {
            DbColumnType.DbInt -> {
                this.setInt(dbIndex, column.element.toInt())
            }
            DbColumnType.DbString -> {
                this.setString(dbIndex, column.element)
            }
        }
    }

}
