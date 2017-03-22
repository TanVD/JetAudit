package tanvd.audit.implementation.mysql

import java.sql.Connection
import java.sql.ResultSet
import java.sql.Statement
import java.util.*
import javax.sql.DataSource
import kotlin.reflect.KClass


/**
 * Provides simple DSL for JDBC clickhouseConnection
 */
class JdbcMysqlConnection(val dataSource: DataSource) {
    /**
     * Creates table with specified header (uses ifNotExists modifier by default)
     */
    fun createTable(tableName: String, tableHeader: List<Pair<String, String>>, primaryKey: String,
                    autoIncrement: Boolean, ifNotExists: Boolean = true) {
        val sqlCreate = StringBuilder()
        sqlCreate.append("CREATE TABLE ")
        sqlCreate.append(if (ifNotExists) "IF NOT EXISTS " else "")
        sqlCreate.append(tableName).append(" (")
        for ((name, type) in tableHeader) {
            sqlCreate.append("$name $type ")
            if (name == primaryKey) {
                sqlCreate.append("NOT NULL ")
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
        } finally {
            stmt?.close()
            connection?.close()
        }
    }

    /**
     * Insert only Int or String. All other values will be ignored.
     * Specify name of inserted value, not actual name of object which this value represents.
     */
    fun insertRow(tableName: String, columns : List<String>, values: List<Pair<String, KClass<*>>>): Int {
        val sqlInsert = StringBuilder()
        sqlInsert.append("INSERT INTO $tableName (${columns.joinToString()}) VALUES (")

        for ((value, type) in values) {
            if (type == String::class) {
                sqlInsert.append("'$value',")
            } else if (type == Int::class) {
                sqlInsert.append("$value,")
            }
        }
        if (sqlInsert.last() == ',') {
            sqlInsert.deleteCharAt(sqlInsert.length - 1)
        }
        sqlInsert.append(");")

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
        val sqlSelect = "SELECT description FROM Audit INNER JOIN $typeName ON $typeName.ID = Audit.ID WHERE " +
                "$typeName.TYPEID = '$id';"

        val rows = ArrayList<String>()

        var connection : Connection? = null
        var stmt : Statement? = null
        var resultSet : ResultSet? = null
        try {
            connection = dataSource.connection
            stmt = connection.createStatement()
            resultSet = stmt.executeQuery(sqlSelect)
            while (resultSet.next()) {
                rows.add(resultSet.getString("description"))
            }
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
        } finally {
            stmt?.close()
            connection?.close()
        }
    }
}
