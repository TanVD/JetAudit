package tanvd.audit.implementation.clickhouse.model

import org.slf4j.LoggerFactory
import tanvd.audit.model.external.types.InformationType
import java.util.*

/**
 * Row of Clickhouse DB.
 */
internal data class DbRow(val columns: List<DbColumn> = ArrayList()) {

    companion object log {
        private val logger = LoggerFactory.getLogger(DbRow::class.java)

    }

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
                DbColumnType.DbDate -> {
                    //Should never come here
                    logger.error("Invoke not implemented function. Should never come here.")
                }
                DbColumnType.DbULong -> {
                    elements[0]
                }
                DbColumnType.DbBoolean -> {
                    if (elements[0].toBoolean()) "1" else "0"
                }
                DbColumnType.DbString -> {
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
    DbDate {
        override fun toString(): String {
            return "Date"
        }
    },
    DbArrayString {
        override fun toString(): String {
            return "Array(String)"
        }
    },
    DbULong {
        override fun toString(): String {
            return "UInt64"
        }
    },
    DbBoolean {
        override fun toString(): String {
            return "UInt8"
        }
    },
    DbString {
        override fun toString(): String {
            return "String"
        }
    },
    DbLong {
        override fun toString(): String {
            return "Int64"
        }
    };

    companion object Factory {
        fun getFromInformationInnerType(type: InformationType.InformationInnerType): DbColumnType {
            when (type) {
                InformationType.InformationInnerType.Long -> {
                    return DbLong
                }
                InformationType.InformationInnerType.String -> {
                    return DbString
                }
                InformationType.InformationInnerType.Boolean -> {
                    return DbBoolean
                }
                InformationType.InformationInnerType.ULong -> {
                    return DbULong
                }
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

internal fun InformationType<*>.toDbColumnHeader(): DbColumnHeader {
    return DbColumnHeader(this.code, DbColumnType.getFromInformationInnerType(this.type))
}