package tanvd.audit.implementation.clickhouse.model

import tanvd.audit.model.external.types.information.InformationType
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
        return columns.map { it.toValues() }.joinToString()
    }

}

/**
 * Column of Clickhouse DB.
 */
internal data class DbColumn(val name: String, val elements: List<String>, val type: DbColumnType) {
    constructor(header: DbColumnHeader, elements: List<String>) : this(header.name, elements, header.type)
    constructor(header: DbColumnHeader, vararg elements: String) : this(header.name, elements.toList(), header.type)

    fun toValues(): String {
        return if (type.isArray) {
            elements.map { valueToSQL(it) }.joinToString(prefix = "[", postfix = "]")
        } else {
            valueToSQL(elements[0])
        }
    }

    private fun valueToSQL(value: String): String {
        return when (type) {
            DbColumnType.DbString -> {
                "\'" + value + "\'"
            }
            DbColumnType.DbBoolean -> {
                if (value.toBoolean()) "1" else "0"
            }
            else -> {
                value
            }

        }
    }
}

/**
 * Clickhouse column type.
 * ToString returns appropriate for db string representation of ColumnType
 */
internal enum class DbColumnType {
    DbDate {
        override val isArray = false

        override fun toString(): String {
            return "Date"
        }
    },
    DbArrayDate {
        override val isArray = true

        override fun toString(): String {
            return "Array(Date)"
        }
    },
    DbULong {
        override val isArray = false

        override fun toString(): String {
            return "UInt64"
        }
    },
    DbArrayULong {
        override val isArray = true

        override fun toString(): String {
            return "Array(UInt64)"
        }
    },
    DbBoolean {
        override val isArray = false

        override fun toString(): String {
            return "UInt8"
        }
    },
    DbArrayBoolean {
        override val isArray = true

        override fun toString(): String {
            return "Array(UInt8)"
        }
    },
    DbString {
        override val isArray = false

        override fun toString(): String {
            return "String"
        }
    },
    DbArrayString {
        override val isArray = true

        override fun toString(): String {
            return "Array(String)"
        }
    },
    DbLong {
        override val isArray = false

        override fun toString(): String {
            return "Int64"
        }
    },
    DbArrayLong {
        override val isArray = true

        override fun toString(): String {
            return "Array(Int64)"
        }
    };

    abstract val isArray: Boolean

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

internal fun InformationType<*>.toDbColumnHeader(): DbColumnHeader {
    return DbColumnHeader(this.code, this.toDbColumnType())
}

internal fun Date.toStringSQL(): String {
    return "'" + getDateFormat().format(java.util.Date(this.time)).toString() + "'"
}

internal fun String.fromSQLtoDate(): java.sql.Date {
    return java.sql.Date(getDateFormat().parse(this).time)
}
