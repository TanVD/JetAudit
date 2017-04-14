package tanvd.audit.implementation.clickhouse.model

import java.util.*

/**
 * Row of Clickhouse DB.
 */
internal data class DbRow(val columns: List<DbColumn> = ArrayList()) {
    fun toStringHeader(): String {
        return columns.map { it.name }.joinToString()
    }

    fun toPlaceholders(): String {
        return columns.map { "?" }.joinToString()
    }

    fun toValues(): String {
        return columns.map { (_, elements, type) ->
            when (type) {
                DbColumnType.DbLong -> {
                    elements[0]
                }
                DbColumnType.DbArrayString -> {
                    elements.map { "\'" + it + "\'" }.joinToString(prefix = "[", postfix = "]")
                }
                else -> {
                    elements[0]
                }
            }
        }.joinToString()
    }

}

/**
 * Column of Clickhouse DB.
 */
internal data class DbColumn(val name: String, val elements: List<String>, val type: DbColumnType) {
    constructor(header: DbColumnHeader, elements: List<String>) : this(header.name, elements, header.type)
    constructor(header: DbColumnHeader, vararg elements: String) : this(header.name, elements.toList(), header.type)
}

/**
 * Clickhouse column type.
 * ToString returns appropriate for db string representation of ColumnType
 */
internal enum class DbColumnType {
    DbDate,
    DbArrayString,
    DbLong;

    override fun toString(): String {
        when (this) {
            DbDate -> {
                return "Date"
            }
            DbArrayString -> {
                return "Array(String)"
            }
            DbLong -> {
                return "UInt64"
            }
        }
    }
}

/**
 * Header for Clickhouse DB
 */
internal data class DbTableHeader(val columnsHeader: List<DbColumnHeader>) {
    /** Returns string definition of TableHeader -- columnFirstName, columnSecondName, ... **/
    fun toDefString(): String {
        return columnsHeader.joinToString { it.toDefString() }
    }

    fun toPlaceholders(): String {
        return columnsHeader.map { "?" }.joinToString()
    }
}

/**
 * Header for Clickhouse Column
 */
internal data class DbColumnHeader(val name: String, val type: DbColumnType) {
    /** Returns string definition of ColumnHeader -- name **/
    fun toDefString(): String {
        return name
    }
}