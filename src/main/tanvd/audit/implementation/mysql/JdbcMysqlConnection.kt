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
import tanvd.audit.model.QueryParameters
import tanvd.audit.model.QueryParameters.OrderByParameters.Order
import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.Statement
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
    fun loadRows(typeName: String, id: String, parameters: QueryParameters): List<DbRow> {
        val sqlSelect = StringBuilder()
        sqlSelect.append("SELECT $descriptionColumn, $unixTimeStampColumn FROM $auditTable INNER JOIN $typeName ON " +
                "$typeName.$auditIdInTypeTable = $auditTable.$auditIdColumn WHERE $typeName.$typeIdColumn = ? ")

        addOrderBy(parameters.orderBy, sqlSelect)

        addLimitPlaceholders(parameters.limits, sqlSelect)

        sqlSelect.append(";")

        val rows = ArrayList<DbRow>()

        var connection: Connection? = null
        var preparedStatement: PreparedStatement? = null
        var resultSet: ResultSet? = null
        try {
            var dbIndex = 1
            connection = dataSource.connection
            preparedStatement = connection.prepareStatement(sqlSelect.toString())

            preparedStatement.setString(dbIndex, id)
            dbIndex++

            dbIndex = setLimits(parameters.limits, preparedStatement, dbIndex)

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

    /**
     * Every append required to end by space to construct right expression with this function
     */
    private fun addLimitPlaceholders(limits: QueryParameters.LimitParameters, sqlSelect: StringBuilder) {
        if (limits.isLimited) {
            sqlSelect.append("LIMIT ?, ? ")
        }
    }

    private fun setLimits(limits: QueryParameters.LimitParameters, preparedStatement: PreparedStatement?, dbIndex: Int): Int {
        if (limits.isLimited && preparedStatement != null) {
            preparedStatement.setInt(dbIndex, limits.limitStart)
            preparedStatement.setInt(dbIndex + 1, limits.limitLength)
            return dbIndex + 2
        }
        return dbIndex
    }

    /**
     * Every append required to end by space to construct right expression with this function
     */
    private fun addOrderBy(orderBy: QueryParameters.OrderByParameters, sqlExpression: StringBuilder) {
        if (orderBy.isOrdered) {
            sqlExpression.append("ORDER BY ${orderBy.codes.joinToString { "${it.first} ${it.second.toStringSQL()}" }} ")
        }
    }

    fun countRows(auditTable: String, typeName: String, id: String): Int {
        var count = 0

        val sqlSelect = "SELECT COUNT(*) FROM $auditTable INNER JOIN $typeName ON " +
                "$typeName.$auditIdInTypeTable = $auditTable.$auditIdColumn WHERE $typeName.$typeIdColumn = ?;"


        var connection: Connection? = null
        var preparedStatement: PreparedStatement? = null
        var resultSet: ResultSet? = null
        try {
            val dbIndex = 1
            connection = dataSource.connection
            preparedStatement = connection.prepareStatement(sqlSelect)

            preparedStatement.setString(dbIndex, id)

            resultSet = preparedStatement.executeQuery()
            if (resultSet.next()) {
                count = resultSet.getInt(1)
            }
        } catch (e: Throwable) {
            logger.error("Error inside MySQL occurred: ", e)
        } finally {
            resultSet?.close()
            preparedStatement?.close()
            connection?.close()
        }
        return count
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

    private fun PreparedStatement.setColumn(column: DbColumn, dbIndex: Int) {
        when (column.type) {
            DbColumnType.DbInt -> {
                this.setInt(dbIndex, column.element.toInt())
            }
            DbColumnType.DbString -> {
                this.setString(dbIndex, column.element)
            }
        }
    }

    private fun Order.toStringSQL(): String {
        return when (this) {
            Order.ASC -> {
                "ASC "
            }
            Order.DESC -> {
                "DESC "
            }
        }
    }


}
