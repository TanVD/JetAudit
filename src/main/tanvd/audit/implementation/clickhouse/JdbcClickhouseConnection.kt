package tanvd.audit.implementation.clickhouse

import org.slf4j.LoggerFactory
import ru.yandex.clickhouse.except.ClickHouseException
import ru.yandex.clickhouse.except.ClickHouseUnknownException
import tanvd.audit.implementation.clickhouse.model.*
import tanvd.audit.implementation.exceptions.BasicDbException
import tanvd.audit.model.external.presenters.IdPresenter
import tanvd.audit.model.external.presenters.VersionPresenter
import tanvd.audit.model.external.queries.*
import tanvd.audit.model.external.queries.QueryParameters.OrderByParameters.Order
import tanvd.audit.model.external.types.information.InformationType
import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.util.*
import javax.sql.DataSource

internal class JdbcClickhouseConnection(val dataSource: DataSource) {

    private val logger = LoggerFactory.getLogger(JdbcClickhouseConnection::class.java)

    private val columnAlreadyCreatedExceptionCode = 44

    /**
     * Creates table with specified header (uses ifNotExists modifier by default)
     * You must specify date field cause we use MergeTree Engine
     *
     * @throws BasicDbException
     */
    fun createTable(tableName: String, tableHeader: DbTableHeader,
                    primaryKey: List<String>, dateKey: String, versionKey: String,
                    setDefaultDate: Boolean = true, ifNotExists: Boolean = true) {
        val sqlCreate = StringBuilder()
        sqlCreate.append("CREATE TABLE ${if (ifNotExists) "IF NOT EXISTS " else ""} ")
        sqlCreate.append(tableHeader.columnsHeader.joinToString(prefix = "$tableName (", postfix = ") ") {
            if (it.name == dateKey && setDefaultDate) {
                "${it.name} ${it.type} DEFAULT today()"
            } else {
                "${it.name} ${it.type}"
            }
        })
        sqlCreate.append("ENGINE = ReplacingMergeTree($dateKey, (${primaryKey.joinToString()}), 8192, $versionKey);")

        var connection: Connection? = null
        var preparedStatement: PreparedStatement? = null
        try {
            connection = dataSource.connection
            preparedStatement = connection.prepareStatement(sqlCreate.toString())
            preparedStatement.executeUpdate()
        } catch (e: Throwable) {
            logger.error("Error inside Clickhouse occurred: ", e)
            throw BasicDbException("Error inside Clickhouse occurred", e)
        } finally {
            preparedStatement?.close()
            connection?.close()
        }
    }

    /**
     * Adds column in a table with specified name
     *
     * @throws BasicDbException
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
            if (e is ClickHouseException && e.errorCode == columnAlreadyCreatedExceptionCode) {
                logger.trace("Trying to create existing column. It will be ignored.", e)
            } else {
                logger.error("Error inside Clickhouse occurred: ", e)
                throw BasicDbException("Error inside Clickhouse occurred", e)
            }
        } finally {
            preparedStatement?.close()
            connection?.close()
        }
    }

    /**
     * Insert only in columns which row contains
     * Inserted values will be sanitized by Clickhouse JDBC driver
     *
     * @throws BasicDbException
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
            throw BasicDbException("Error inside Clickhouse occurred", e)
        } finally {
            preparedStatement?.close()
            connection?.close()
        }
    }

    /**
     * Inserts in all columns (Some maybe not stated).
     * Uses batches. Inserted values will be sanitized by Clickhouse JDBC driver
     *
     * @throws BasicDbException
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
            throw BasicDbException("Error inside Clickhouse occurred", e)
        } finally {
            preparedStatement?.close()
            connection?.close()
        }
    }

    /**
     * Loads rows satisfying expression with all specified columns from specified table.
     * Parameters will be applied to query (like limit, or order by)
     *
     * @throws BasicDbException
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
            connection = dataSource.connection
            preparedStatement = connection.prepareStatement(sqlSelect.toString())

            var dbIndex = 1
            dbIndex = setQueryIds(expression, preparedStatement, dbIndex)
            setLimits(parameters.limits, preparedStatement, dbIndex)

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
            throw BasicDbException("Error inside Clickhouse occurred", e)
        } finally {
            resultSet?.close()
            preparedStatement?.close()
            connection?.close()
        }

        val distinctRows = rows.groupBy { (columns) ->
            columns.find { (name) ->
                name == InformationType.resolveType(IdPresenter).code
            }!!.elements[0].toLong()
        }.
                mapValues {
                    it.value.sortedByDescending { (columns) ->
                        columns.find { (name) -> name == InformationType.resolveType(VersionPresenter).code }!!.
                                elements[0].toLong()
                    }.first()
                }.values.toList()


        return distinctRows
    }

    /**
     * Drops specified table
     *
     * @throws BasicDbException
     */
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
            throw BasicDbException("Error inside Clickhouse occurred", e)
        } finally {
            preparedStatement?.close()
            connection?.close()
        }
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
            is QueryTypeStringLeaf -> {
                expression.toStringSQL()
            }
            is QueryTypeLongLeaf -> {
                expression.toStringSQL()
            }
            is QueryTypeBooleanLeaf -> {
                expression.toStringSQL()
            }
            is QueryInformationStringLeaf -> {
                expression.toStringSQL()
            }
            is QueryInformationLongLeaf -> {
                expression.toStringSQL()
            }
            is QueryInformationBooleanLeaf -> {
                expression.toStringSQL()
            }
            else -> {
                logger.error("Unknown Query leaf.")
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
            is QueryTypeStringLeaf -> {
                preparedStatement?.setString(dbIndexVar, expression.value)
                dbIndexVar++
            }
            is QueryInformationStringLeaf -> {
                preparedStatement?.setString(dbIndexVar, expression.value)
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
        val allOrder = orderBy.codesState.map { it.key.getCode() to it.value }.toMutableList()
        allOrder.addAll(orderBy.codesInformation.map { it.key.code to it.value })
        if (orderBy.isOrdered) {
            sqlExpression.append("ORDER BY ${allOrder.joinToString { "${it.first} ${it.second.toStringSQL()}" }} ")
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
            connection = dataSource.connection
            preparedStatement = connection.prepareStatement(sqlSelect.toString())

            setQueryIds(expression, preparedStatement, 1)

            resultSet = preparedStatement.executeQuery()

            if (resultSet.next()) {
                count = resultSet.getInt(1)
            }
        } catch (e: Throwable) {
            logger.error("Error inside Clickhouse occurred: ", e)
            throw BasicDbException("Error inside Clickhouse occurred", e)
        } finally {
            resultSet?.close()
            preparedStatement?.close()
            connection?.close()
        }


        return count
    }


    private fun PreparedStatement.setColumn(column: DbColumn, dbIndex: Int) {
        when (column.type) {
            DbColumnType.DbArrayString -> {
                this.setArray(dbIndex,
                        connection.createArrayOf("String", column.elements.toTypedArray()))
            }
            DbColumnType.DbDate -> {
                logger.error("Default field got in insert. Scheme violation.")
            }
            DbColumnType.DbLong -> {
                this.setLong(dbIndex, column.elements[0].toLong())
            }
            DbColumnType.DbULong -> {
                this.setLong(dbIndex, column.elements[0].toLong())
            }
            DbColumnType.DbBoolean -> {
                this.setInt(dbIndex, if (column.elements[0].toBoolean()) 1 else 0)
            }
            DbColumnType.DbString -> {
                this.setString(dbIndex, column.elements[0])
            }
            DbColumnType.DbArrayULong -> {
                this.setArray(dbIndex,
                        connection.createArrayOf("UInt64", column.elements.map { it.toLong() }.toTypedArray()))
            }
            DbColumnType.DbArrayBoolean -> {
                this.setArray(dbIndex,
                        connection.createArrayOf("UInt8", column.elements.map {
                            if (it.toBoolean()) 1 else 0
                        }.toTypedArray()))
            }
            DbColumnType.DbArrayLong -> {
                this.setArray(dbIndex,
                        connection.createArrayOf("Int64", column.elements.map { it.toLong() }.toTypedArray()))
            }
        }
    }

    private fun ResultSet.getColumn(column: DbColumnHeader): DbColumn {
        when (column.type) {
            DbColumnType.DbDate -> {
                logger.error("Default field got in loadPropertiesFromFile. Scheme violation.")
                return DbColumn("", emptyList(), DbColumnType.DbArrayString)
            }
            DbColumnType.DbLong -> {
                val result = this.getLong(column.name)
                return DbColumn(column.name, listOf(result.toString()), DbColumnType.DbLong)
            }
            DbColumnType.DbArrayLong -> {
                @Suppress("UNCHECKED_CAST")
                val resultArray = (this.getArray(column.name).array as LongArray).map { it.toString() }
                return DbColumn(column.name, resultArray.toList(), DbColumnType.DbArrayLong)
            }
            DbColumnType.DbULong -> {
                val result = this.getLong(column.name)
                return DbColumn(column.name, listOf(result.toString()), DbColumnType.DbLong)
            }
            DbColumnType.DbArrayULong -> {
                @Suppress("UNCHECKED_CAST")
                val resultArray = (this.getArray(column.name).array as LongArray).map { it.toString() }
                return DbColumn(column.name, resultArray.toList(), DbColumnType.DbArrayULong)
            }
            DbColumnType.DbBoolean -> {
                val result = this.getInt(column.name)
                return DbColumn(column.name, listOf(if (result == 1) true.toString() else false.toString()), DbColumnType.DbBoolean)
            }
            DbColumnType.DbArrayBoolean -> {
                @Suppress("UNCHECKED_CAST")
                val resultArray = (this.getArray(column.name).array as LongArray).map {
                    if (it.toInt() == 1)
                        true.toString()
                    else
                        false.toString()
                }
                return DbColumn(column.name, resultArray.toList(), DbColumnType.DbArrayBoolean)
            }
            DbColumnType.DbString -> {
                val result = this.getString(column.name)
                return DbColumn(column.name, listOf(result), DbColumnType.DbString)
            }
            DbColumnType.DbArrayString -> {
                @Suppress("UNCHECKED_CAST")
                val resultArray = this.getArray(column.name).array as Array<String>
                return DbColumn(column.name, resultArray.toList(), DbColumnType.DbArrayString)
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
