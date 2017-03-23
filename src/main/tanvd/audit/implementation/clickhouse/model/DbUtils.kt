package tanvd.audit.implementation.clickhouse.model

import java.util.*

/**
 * Row of Clickhouse DB.
 */
data class DbRow(val columns : List<DbColumn> = ArrayList()) {
    fun toStringHeader() : String {
        return columns.map { it.name }.joinToString()
    }

    fun toStringValues() : String {
        return this.columns.joinToString { it.toStringValue() }
    }
}

/**
 * Column of Clickhouse DB.
 */
data class DbColumn(val name: String, val elements : List<String>, val type : DbColumnType) {
    fun toStringValue() : String {
        when (type) {
            DbColumnType.DbDate -> {
                return elements[0]
            }
            DbColumnType.DbString -> {
                return "'${elements[0]}'"
            }
            DbColumnType.DbArrayString -> {
                return "[${elements.joinToString { "'$it'" }}]"
            }
        }
    }
}

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
data class DbTableHeader(val columnsHeader : List<DbColumnHeader>) {
    /** Returns string definition of TableHeader -- columnFirstName, columnSecondName, ... **/
    fun toDefString() : String {
        return columnsHeader.joinToString { it.toDefString() }
    }
}

/**
 * Header for Clickhouse Column
 */
data class DbColumnHeader(val name : String, val type : DbColumnType) {
    /** Returns string definition of ColumnHeader -- name **/
    fun toDefString() : String {
        return name
    }
}