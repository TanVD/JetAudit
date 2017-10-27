package tanvd.audit.implementation.clickhouse

import org.slf4j.LoggerFactory
import ru.yandex.clickhouse.except.ClickHouseException
import tanvd.audit.implementation.clickhouse.jdbc.PreparedCHStatement
import tanvd.audit.implementation.clickhouse.model.*
import tanvd.audit.implementation.exceptions.BasicDbException
import tanvd.audit.model.external.presenters.IdPresenter
import tanvd.audit.model.external.presenters.VersionPresenter
import tanvd.audit.model.external.queries.*
import tanvd.audit.model.external.types.information.InformationType
import tanvd.audit.utils.PropertyLoader
import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.util.*
import javax.sql.DataSource

internal class JdbcClickhouseConnection(val dataSource: DataSource) {

    private val logger = LoggerFactory.getLogger(JdbcClickhouseConnection::class.java)

    private val columnAlreadyCreatedExceptionCode = 44

    private val timeout = PropertyLoader["ConnectionTimeout"]?.toInt() ?: 2000

    private var connection: Connection? = null

    private fun getConnection(): Connection {
        if (connection == null || connection?.isValid(timeout) != true) {
            connection = dataSource.connection
        }
        return connection!!
    }

    /**
     * Creates table with specified header (uses ifNotExists modifier by default)
     * You must specify date field cause we use MergeTree Engine
     *
     * @throws BasicDbException
     */
    fun createTable(tableName: String, tableHeader: DbTableHeader,
                    primaryKey: List<String>, dateKey: String, versionKey: String,
                    ifNotExists: Boolean = true) {
        val sqlCreate = StringBuilder()
        sqlCreate.append("CREATE TABLE ${if (ifNotExists) "IF NOT EXISTS " else ""} ")
        sqlCreate.append(tableHeader.columnsHeader.joinToString(prefix = "$tableName (", postfix = ") ") {
            "${it.name} ${it.type}"
        })
        sqlCreate.append("ENGINE = ReplacingMergeTree($dateKey, (${primaryKey.joinToString()}), 8192, $versionKey);")

        val connection: Connection?
        var preparedStatement: PreparedStatement? = null
        try {
            connection = getConnection()
            preparedStatement = connection.prepareStatement(sqlCreate.toString())
            preparedStatement.executeUpdate()
        } catch (e: Throwable) {
            logger.error("Error inside Clickhouse occurred: ", e)
            throw BasicDbException("Error inside Clickhouse occurred", e)
        } finally {
            preparedStatement?.close()
        }
    }

    /**
     * Adds column in a table with specified name
     *
     * @throws BasicDbException
     */
    fun addColumn(tableName: String, columnHeader: DbColumnHeader) {
        val sqlAlter = "ALTER TABLE $tableName ADD COLUMN ${columnHeader.name} ${columnHeader.type} ;"
        val connection: Connection?
        var preparedStatement: PreparedStatement? = null
        try {
            connection = getConnection()
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

        val connection: Connection?
        var preparedStatement: PreparedStatement? = null
        try {
            connection = getConnection()
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
        }
    }

    /**
     * Inserts in all columns (Some maybe not stated).
     * Uses batches. Inserted values will be sanitized by Clickhouse JDBC driver
     *
     * @throws BasicDbException
     */
    fun insertRows(tableName: String, tableHeader: DbTableHeader, rows: List<DbRow>) {
        val connection: Connection?
        var preparedStatement: PreparedStatement? = null
        try {
            connection = getConnection()
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
        val rows = ArrayList<DbRow>()

        val connection: Connection?
        var preparedStatement: PreparedStatement? = null
        var resultSet: ResultSet? = null
        try {
            connection = getConnection()
            preparedStatement = PreparedCHStatement(connection,
                    " SELECT ${columnsToSelect.toDefString()} ",
                    " FROM $tableName ",
                    prewhereSection = expression,
                    parametersSection = parameters).prepare()
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
        }


        return rows.groupBy { (columns) ->
            columns.find { (name) ->
                name == InformationType.resolveType(IdPresenter).code
            }!!.elements[0].toLong()
        }.mapValues {
            it.value.sortedByDescending { (columns) ->
                columns.find { (name) -> name == InformationType.resolveType(VersionPresenter).code }!!.
                        elements[0].toLong()
            }.first()
        }.values.toList()
    }

    fun loadRows(sql: String, columnsToSelect: DbTableHeader): List<DbRow> {
        val rows = ArrayList<DbRow>()

        val connection: Connection?
        var preparedStatement: PreparedStatement? = null
        var resultSet: ResultSet? = null
        try {
            connection = getConnection()
            preparedStatement = connection.prepareStatement(sql)

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
        }


        return rows.groupBy { (columns) ->
            columns.find { (name) ->
                name == InformationType.resolveType(IdPresenter).code
            }!!.elements[0].toLong()
        }.mapValues {
            it.value.sortedByDescending { (columns) ->
                columns.find { (name) -> name == InformationType.resolveType(VersionPresenter).code }!!.
                        elements[0].toLong()
            }.first()
        }.values.toList()
    }

    /**
     * Drops specified table
     *
     * @throws BasicDbException
     */
    fun dropTable(tableName: String, ifExists: Boolean) {
        val sqlDrop = "DROP TABLE ${if (ifExists) "IF EXISTS" else ""} $tableName;"

        val connection: Connection?
        var preparedStatement: PreparedStatement? = null
        try {
            connection = getConnection()
            preparedStatement = connection.prepareStatement(sqlDrop)
            preparedStatement.executeUpdate()
        } catch (e: Throwable) {
            logger.error("Error inside Clickhouse occurred: ", e)
            throw BasicDbException("Error inside Clickhouse occurred", e)
        } finally {
            preparedStatement?.close()
        }
    }




    fun countRows(tableName: String, expression: QueryExpression): Long {
        var count = 0L
        val connection: Connection?
        var preparedStatement: PreparedStatement? = null
        var resultSet: ResultSet? = null
        try {
            connection = getConnection()
            preparedStatement = PreparedCHStatement(connection,
                    " SELECT COUNT(*) ",
                    " FROM $tableName ",
                    prewhereSection = expression).prepare()

            resultSet = preparedStatement.executeQuery()

            if (resultSet.next()) {
                count = resultSet.getLong(1)
            }
        } catch (e: Throwable) {
            logger.error("Error inside Clickhouse occurred: ", e)
            throw BasicDbException("Error inside Clickhouse occurred", e)
        } finally {
            resultSet?.close()
            preparedStatement?.close()
        }


        return count
    }

    fun close() {
        connection?.close()
    }


    private fun PreparedStatement.setColumn(column: DbColumn, dbIndex: Int) {
        when (column.type) {
            DbColumnType.DbDate -> {
                this.setDate(dbIndex, column.elements[0].toSqlDate())
            }
            DbColumnType.DbArrayDate -> {
                this.setArray(dbIndex,
                        connection.createArrayOf("Date", column.elements.map {
                            column.elements[0].toSqlDate()
                        }.toTypedArray()))
            }
            DbColumnType.DbDateTime -> {
                this.setTimestamp(dbIndex, column.elements[0].toSqlTimestamp())
            }
            DbColumnType.DbArrayDateTime -> {
                this.setArray(dbIndex,
                        connection.createArrayOf("DateTime", column.elements.map {
                            column.elements[0].toSqlTimestamp()
                        }.toTypedArray()))
            }
            DbColumnType.DbLong -> {
                this.setLong(dbIndex, column.elements[0].toLong())
            }
            DbColumnType.DbArrayLong -> {
                this.setArray(dbIndex,
                        connection.createArrayOf("Int64", column.elements.map { it.toLong() }.toTypedArray()))
            }
            DbColumnType.DbULong -> {
                this.setLong(dbIndex, column.elements[0].toLong())
            }
            DbColumnType.DbArrayULong -> {
                this.setArray(dbIndex,
                        connection.createArrayOf("UInt64", column.elements.map { it.toLong() }.toTypedArray()))
            }
            DbColumnType.DbBoolean -> {
                this.setInt(dbIndex, if (column.elements[0].toBoolean()) 1 else 0)
            }
            DbColumnType.DbArrayBoolean -> {
                this.setArray(dbIndex,
                        connection.createArrayOf("UInt8", column.elements.map {
                            if (it.toBoolean()) 1 else 0
                        }.toTypedArray()))
            }
            DbColumnType.DbString -> {
                this.setString(dbIndex, column.elements[0])
            }
            DbColumnType.DbArrayString -> {
                this.setArray(dbIndex,
                        connection.createArrayOf("String", column.elements.toTypedArray()))
            }
        }
    }

    private fun ResultSet.getColumn(column: DbColumnHeader): DbColumn {
        when (column.type) {
            DbColumnType.DbDate -> {
                val dateSerialized = getDate(column.name).toStringFromDb()
                return DbColumn(column.name, listOf(dateSerialized), DbColumnType.DbDate)
            }
            DbColumnType.DbArrayDate -> {
                @Suppress("UNCHECKED_CAST")
                val resultArray = (getArray(column.name).array as Array<Date>).map {
                    getDate(column.name).toStringFromDb()
                }
                return DbColumn(column.name, resultArray.toList(), DbColumnType.DbArrayDate)
            }
            DbColumnType.DbDateTime -> {
                val dateSerialized = getTimestamp(column.name).toStringFromDb()
                return DbColumn(column.name, listOf(dateSerialized), DbColumnType.DbDateTime)
            }
            DbColumnType.DbArrayDateTime -> {
                @Suppress("UNCHECKED_CAST")
                val resultArray = (getArray(column.name).array as Array<Date>).map {
                    getTimestamp(column.name).toStringFromDb()
                }
                return DbColumn(column.name, resultArray.toList(), DbColumnType.DbArrayDateTime)
            }
            DbColumnType.DbLong -> {
                val result = getLong(column.name)
                return DbColumn(column.name, listOf(result.toString()), DbColumnType.DbLong)
            }
            DbColumnType.DbArrayLong -> {
                @Suppress("UNCHECKED_CAST")
                val resultArray = (getArray(column.name).array as LongArray).map { it.toString() }
                return DbColumn(column.name, resultArray.toList(), DbColumnType.DbArrayLong)
            }
            DbColumnType.DbULong -> {
                val result = getLong(column.name)
                return DbColumn(column.name, listOf(result.toString()), DbColumnType.DbLong)
            }
            DbColumnType.DbArrayULong -> {
                @Suppress("UNCHECKED_CAST")
                val resultArray = (getArray(column.name).array as LongArray).map { it.toString() }
                return DbColumn(column.name, resultArray.toList(), DbColumnType.DbArrayULong)
            }
            DbColumnType.DbBoolean -> {
                val result = getInt(column.name)
                return DbColumn(column.name, listOf(if (result == 1) true.toString() else false.toString()), DbColumnType.DbBoolean)
            }
            DbColumnType.DbArrayBoolean -> {
                @Suppress("UNCHECKED_CAST")
                val resultArray = (getArray(column.name).array as LongArray).map {
                    if (it.toInt() == 1)
                        true.toString()
                    else
                        false.toString()
                }
                return DbColumn(column.name, resultArray.toList(), DbColumnType.DbArrayBoolean)
            }
            DbColumnType.DbString -> {
                val result = getString(column.name)
                return DbColumn(column.name, listOf(result), DbColumnType.DbString)
            }
            DbColumnType.DbArrayString -> {
                @Suppress("UNCHECKED_CAST")
                val resultArray = getArray(column.name).array as Array<String>
                return DbColumn(column.name, resultArray.toList(), DbColumnType.DbArrayString)
            }
        }
    }
}
