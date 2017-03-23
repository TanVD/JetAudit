package tanvd.audit.implementation.mysql

import org.slf4j.LoggerFactory
import tanvd.audit.implementation.mysql.AuditDaoMysqlImpl.Config.auditIdColumn
import tanvd.audit.implementation.mysql.AuditDaoMysqlImpl.Config.auditIdInTypeTable
import tanvd.audit.implementation.mysql.AuditDaoMysqlImpl.Config.auditTable
import tanvd.audit.implementation.mysql.AuditDaoMysqlImpl.Config.descriptionColumn
import tanvd.audit.implementation.mysql.AuditDaoMysqlImpl.Config.typeIdColumn
import tanvd.audit.implementation.mysql.model.DbRow
import tanvd.audit.implementation.mysql.model.DbTableHeader
import java.sql.Connection
import java.sql.ResultSet
import java.sql.SQLException
import java.sql.Statement
import java.util.*
import javax.sql.DataSource
import kotlin.reflect.KClass


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
        sqlCreate.append("CREATE TABLE ")
        sqlCreate.append(if (ifNotExists) "IF NOT EXISTS " else "")
        sqlCreate.append(tableName).append(" (")
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

        var connection : Connection? = null
        var stmt : Statement? = null
        try {
            connection = dataSource.connection
            stmt = connection.createStatement()
            stmt.execute(sqlCreate.toString())
        } catch (e : SQLException) {
            logger.error("Error inside MySQL occurred: ", e)
        } finally {
            stmt?.close()
            connection?.close()
        }
    }

    /**
     * Insert only Int or String. All other values will be ignored.
     * Specify name of inserted value, not actual name of object which this value represents.
     */
    fun insertRow(tableName: String, row : DbRow): Int {
        val sqlInsert = StringBuilder()
        sqlInsert.append("INSERT INTO $tableName (${row.toStringHeader()}) VALUES (${row.toStringValues()});")

        var connection : Connection? = null
        var stmt : Statement? = null
        var resultSet : ResultSet? = null
        var id = -1
        try {
            connection = dataSource.connection
            stmt = connection.prepareStatement(sqlInsert.toString(), Statement.RETURN_GENERATED_KEYS)
            stmt.executeUpdate()
            resultSet = stmt.generatedKeys

            if (resultSet.next()) {
                id = resultSet.getInt(1)
            }
        } catch (e : SQLException) {
            logger.error("Error inside MySQL occurred: ", e)
        } finally {
            resultSet?.close()
            stmt?.close()
            connection?.close()
        }

        return id
    }

    /**
     * Loads all audits connected to this object
     */
    fun loadRows(typeName : String, id : String) : List<String>{
        val sqlSelect = "SELECT $descriptionColumn FROM $auditTable INNER JOIN $typeName ON " +
                "$typeName.$auditIdInTypeTable = $auditTable.$auditIdColumn WHERE $typeName.$typeIdColumn = '$id';"

        val rows = ArrayList<String>()

        var connection : Connection? = null
        var stmt : Statement? = null
        var resultSet : ResultSet? = null
        try {
            connection = dataSource.connection
            stmt = connection.createStatement()
            resultSet = stmt.executeQuery(sqlSelect)
            while (resultSet.next()) {
                rows.add(resultSet.getString(descriptionColumn))
            }
        } catch (e : SQLException) {
            logger.error("Error inside MySQL occurred: ", e)
        } finally {
            resultSet?.close()
            stmt?.close()
            connection?.close()
        }
        return rows
    }

    fun dropTable(tableName : String, ifExists : Boolean) {
        val sqlDrop = "DROP TABLE ${if (ifExists) "IF EXISTS" else ""} $tableName;"

        var connection : Connection? = null
        var stmt : Statement? = null
        try {
            connection = dataSource.connection
            stmt = connection.createStatement()
            stmt.executeUpdate(sqlDrop)
        } catch (e : SQLException) {
            logger.error("Error inside MySQL occurred: ", e)
        } finally {
            stmt?.close()
            connection?.close()
        }
    }
}
