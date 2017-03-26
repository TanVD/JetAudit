package tanvd.audit.implementation.clickhouse

import org.slf4j.LoggerFactory
import tanvd.audit.implementation.clickhouse.AuditDaoClickhouseImpl.Scheme.unixTimeStampColumn
import tanvd.audit.implementation.clickhouse.model.*
import tanvd.audit.model.external.*
import tanvd.audit.model.external.QueryParameters.OrderByParameters.Order
import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.util.*
import javax.sql.DataSource

/** Be aware that all exceptions in Clickhouse will be saved to the log as errors and than ignored. **/
internal class JdbcClickhouseConnection(val dataSource: DataSource) {

    private val logger = LoggerFactory.getLogger(JdbcClickhouseConnection::class.java)


    /**
     * Creates table with specified header (uses ifNotExists modifier by default)
     * You must specify date field cause we use MergeTree Engine
     */
    fun createTable(tableName: String, tableHeader: DbTableHeader, primaryKey: String, dateKey: String,
                    setDefaultDate: Boolean = true, ifNotExists: Boolean = true) {
        val sqlCreate = StringBuilder()
        sqlCreate.append("CREATE TABLE ${if (ifNotExists) "IF NOT EXISTS " else ""} ")
        sqlCreate.append(tableHeader.columnsHeader.joinToString(prefix = "$tableName (", postfix = ") ") {
            if (it.name == dateKey && setDefaultDate)
                "${it.name} ${it.type} DEFAULT today()"
            else
                "${it.name} ${it.type}"
        })
        sqlCreate.append("ENGINE = MergeTree($dateKey, ($primaryKey), 8192);")

        var connection: Connection? = null
        var preparedStatement: PreparedStatement? = null
        try {
            connection = dataSource.connection
            preparedStatement = connection.prepareStatement(sqlCreate.toString())
            preparedStatement.executeUpdate()
        } catch (e: Throwable) {
            logger.error("Error inside Clickhouse occurred: ", e)
        } finally {
            preparedStatement?.close()
            connection?.close()
        }
    }

    /**
     * Adds column in a table with specified name and name
     */
    fun addColumn(tableName: String, columnHeader: DbColumnHeader) {
        val sqlAlter = "ALTER TABLE $tableName ADD COLUMN ${columnHeader.name} ${columnHeader.type} ;"

        var connection: Connection? = null
        var preparedStatement: PreparedStatement? = null
        try {
            connection = dataSource.connection
            preparedStatement = connection.prepareStatement(sqlAlter)
            preparedStatement.executeUpdate()
        } catch (e: Throwable) {
            logger.error("Error inside Clickhouse occurred: ", e)
        } finally {
            preparedStatement?.close()
            connection?.close()
        }
    }

    /**
     * Insert only in columns which row contains
     * Inserted values will sanitized by Clickhouse JDBC driver
     */
    fun insertRow(tableName: String, row: DbRow) {
        val sqlInsert = "INSERT INTO $tableName (${row.toStringHeader()}) VALUES (${row.toPlaceholders()});"

        var connection: Connection? = null
        var preparedStatement: PreparedStatement? = null
        try {
            connection = dataSource.connection
            preparedStatement = connection.prepareStatement(sqlInsert)
            for ((index, column) in row.columns.withIndex()) {
                preparedStatement.setColumn(column, index + 1)
            }
            preparedStatement.executeUpdate()
        } catch (e: Throwable) {
            logger.error("Error inside Clickhouse occurred: ", e)
        } finally {
            preparedStatement?.close()
            connection?.close()
        }
    }

    /**
     * Insert in all columns (Some maybe not stated).
     * Use batches. Inserted values will be sanitized by Clickhouse JDBC driver
     */
    fun insertRows(tableName: String, tableHeader: DbTableHeader, rows: List<DbRow>) {
        var connection: Connection? = null
        var preparedStatement: PreparedStatement? = null
        try {
            connection = dataSource.connection
            preparedStatement = connection.prepareStatement("INSERT INTO $tableName (${tableHeader.toDefString()})" +
                    " VALUES (${tableHeader.toPlaceholders()});")

            for ((columns) in rows) {
                for ((index, column) in tableHeader.columnsHeader.withIndex()) {
                    val dbIndex = index + 1
                    val appropriateColumn = columns.find { it.name == column.name }
                    if (appropriateColumn != null) {
                        preparedStatement.setColumn(appropriateColumn, dbIndex)
                    } else {
                        preparedStatement.setColumn(DbColumn("", emptyList(), DbColumnType.DbArrayString), dbIndex)
                    }
                }
                preparedStatement.addBatch()
            }

            preparedStatement?.executeBatch()
        } catch (e: Throwable) {
            logger.error("Error inside Clickhouse occurred: ", e)
        } finally {
            preparedStatement?.close()
            connection?.close()
        }
    }

    /**
     * Loads rows with all specified columns from specified table where column typeName has element id
     * Id will be sanitized by Clickhouse JDBC driver
     */
    fun loadRows(tableName: String, columnsToSelect: DbTableHeader, expression: QueryExpression,
                 parameters: QueryParameters): List<DbRow> {
        val sqlSelect = StringBuilder()
        sqlSelect.append("SELECT ${columnsToSelect.toDefString()} FROM $tableName WHERE ")

        addExpression(expression, sqlSelect)

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

            dbIndex = setQueryIds(expression, preparedStatement, dbIndex)

            dbIndex = setLimits(parameters.limits, preparedStatement, dbIndex)

            resultSet = preparedStatement.executeQuery()

            while (resultSet.next()) {
                val elements = ArrayList<DbColumn>()
                for (columnHeader in columnsToSelect.columnsHeader) {
                    elements.add(resultSet.getColumn(columnHeader))
                }
                rows.add(DbRow(elements))
            }
        } catch (e: Throwable) {
            logger.error("Error inside Clickhouse occurred: ", e)
        } finally {
            resultSet?.close()
            preparedStatement?.close()
            connection?.close()
        }


        return rows
    }

    private fun addExpression(expression: QueryExpression, sqlSelect: StringBuilder) {
        sqlSelect.append(serializeExpression(expression))
        sqlSelect.append(" ")
    }

    private fun serializeExpression(expression: QueryExpression): String {
        return when (expression) {
            is QueryNode -> {
                when (expression.queryOperator) {
                    QueryOperator.and -> {
                        "(${serializeExpression(expression.expressionFirst)}) AND " +
                                "(${serializeExpression(expression.expressionSecond)})"
                    }
                    QueryOperator.or -> {
                        "(${serializeExpression(expression.expressionFirst)}) OR " +
                                "(${serializeExpression(expression.expressionSecond)})"
                    }
                }
            }
            is QueryTypeLeaf -> {
                expression.toStringSQL()
            }
            is QueryTimeStampLeaf -> {
                expression.toStringSQL()
            }
            else -> {
                ""
            }
        }
    }

    private fun setQueryIds(expression: QueryExpression, preparedStatement: PreparedStatement?, dbIndex: Int): Int {
        var dbIndexVar = dbIndex
        when (expression) {
            is QueryNode -> {
                dbIndexVar = setQueryIds(expression.expressionFirst, preparedStatement, dbIndexVar)
                dbIndexVar = setQueryIds(expression.expressionSecond, preparedStatement, dbIndexVar)
            }
            is QueryTypeLeaf -> {
                preparedStatement?.setString(dbIndexVar, expression.id)
                dbIndexVar++
            }
        }
        return dbIndexVar
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


    fun countRows(tableName: String, expression: QueryExpression): Int {
        var count = 0

        val sqlSelect = StringBuilder()

        sqlSelect.append("SELECT COUNT(*) FROM $tableName WHERE ")

        addExpression(expression, sqlSelect)

        sqlSelect.append(";")

        var connection: Connection? = null
        var preparedStatement: PreparedStatement? = null
        var resultSet: ResultSet? = null
        try {
            var dbIndex = 1
            connection = dataSource.connection
            preparedStatement = connection.prepareStatement(sqlSelect.toString())

            dbIndex = setQueryIds(expression, preparedStatement, dbIndex)

            resultSet = preparedStatement.executeQuery()

            if (resultSet.next()) {
                count = resultSet.getInt(1)
            }
        } catch (e: Throwable) {
            logger.error("Error inside Clickhouse occurred: ", e)
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
        var preparedStatement: PreparedStatement? = null
        try {
            connection = dataSource.connection
            preparedStatement = connection.prepareStatement(sqlDrop)
            preparedStatement.executeUpdate()
        } catch (e: Throwable) {
            logger.error("Error inside Clickhouse occurred: ", e)
        } finally {
            preparedStatement?.close()
            connection?.close()
        }
    }

    private fun PreparedStatement.setColumn(column: DbColumn, dbIndex: Int) {
        when (column.type) {
            DbColumnType.DbString -> {
                this.setString(dbIndex, column.elements[0])
            }
            DbColumnType.DbArrayString -> {
                this.setArray(dbIndex,
                        connection.createArrayOf("String", column.elements.toTypedArray()))
            }
            DbColumnType.DbDate -> {
                logger.error("Default field got in insert. Scheme violation.")
            }
            DbColumnType.DbInt -> {
                this.setInt(dbIndex, column.elements[0].toInt())
            }
        }
    }

    private fun ResultSet.getColumn(column: DbColumnHeader): DbColumn {
        when (column.type) {
            DbColumnType.DbDate -> {
                logger.error("Default field got in load. Scheme violation.")
                return DbColumn("", emptyList(), DbColumnType.DbArrayString)
            }
            DbColumnType.DbArrayString -> {
                @Suppress("UNCHECKED_CAST")
                val resultArray = this.getArray(column.name).array as Array<String>
                return DbColumn(column.name, resultArray.toList(), DbColumnType.DbArrayString)
            }
            DbColumnType.DbString -> {
                val result = this.getString(column.name)
                return DbColumn(column.name, listOf(result), DbColumnType.DbString)
            }
            DbColumnType.DbInt -> {
                val result = this.getInt(column.name)
                return DbColumn(column.name, listOf(result.toString()), DbColumnType.DbInt)
            }
        }
    }

    private fun QueryTimeStampLeaf.toStringSQL(): String {
        return when (condition) {
            QueryTimeStampCondition.less -> {
                "$unixTimeStampColumn < $timeStamp"
            }
            QueryTimeStampCondition.more -> {
                "$unixTimeStampColumn > $timeStamp"
            }
            QueryTimeStampCondition.equal -> {
                "$unixTimeStampColumn = $timeStamp"
            }
        }
    }

    private fun QueryTypeLeaf.toStringSQL(): String {
        return when (typeCondition) {
            QueryTypeCondition.equal -> {
                "has(${type.code}, ?)"
            }
        //TODO Here we can use like, but it will work only for expression like %something%
            QueryTypeCondition.like -> {
                "arrayExists((x) -> match(x, ?), ${type.code})"
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
