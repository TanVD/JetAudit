package tanvd.audit.implementation.clickhouse.model

import java.util.*

/**
 * Row of Clickhouse DB.
 */
data class DbRow(val columns : MutableList<DbColumn> = ArrayList()) {
    fun toStringInsert() : String {
        return this.columns.joinToString(prefix = "(", postfix = ")") { it.toStringInsert() }
    }
}

/**
 * Column of Clickhouse DB.
 */
data class DbColumn(val name: String, val elements : List<String>, val type : DbColumnType) {
    fun toStringInsert() : String {
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
data class DbTableHeader(val columnsHeader : MutableList<DbColumnHeader>) {
    fun toStringInsert() : String {
        return columnsHeader.joinToString(prefix = "(", postfix = ")") { it.toStringInsert() }
    }
}

/**
 * Header for Clickhouse Column
 */
data class DbColumnHeader(val name : String, val type : DbColumnType) {
    fun toStringInsert() : String {
        return name
    }
}