package tanvd.audit.implementation.clickhouse.model

import java.util.*

/**
 * Row of Clickhouse DB.
 */
data class DbRow(val columns : MutableList<DbColumn> = ArrayList())

/**
 * Column of Clickhouse DB.
 */
data class DbColumn(val name: String, val elements : List<String>, val type : DbColumnType)

/**
 * Clickhouse column type.
 * ToString returns appropriate string representation of ColumnType
 */
enum class DbColumnType {
    DbDate,
    DbArrayString,
    DbString;

    override fun toString() : String {
        when (this) {
            DbDate -> {
                return "Date"
            }
            DbArrayString -> {
                return "Array(String)"
            }
            DbString -> {
                return "String"
            }
        }
    }
}

/**
 * Header for Clickhouse DB
 */
data class DbTableHeader(val columnsHeader : MutableList<DbColumnHeader>)

/**
 * Header for Clickhouse Column
 */
data class DbColumnHeader(val name : String, val type : DbColumnType)