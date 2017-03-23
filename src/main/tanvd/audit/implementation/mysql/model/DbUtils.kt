package tanvd.audit.implementation.mysql.model

import java.util.ArrayList


/**
 * Row of MySQL DB.
 */
data class DbRow(val columns : List<DbColumn>) {
    fun toStringHeader() : String {
        return columns.map { it.name }.joinToString()
    }

    fun toStringValues() : String {
        return this.columns.joinToString { it.toStringValue() }
    }
}

/**
 * Column of MySQL DB.
 */
data class DbColumn(val name: String, val element: String, val type : DbColumnType) {
    fun toStringValue() : String {
        when (type) {
            DbColumnType.DbInt -> {
                return element
            }
            DbColumnType.DbString -> {
                return "'$element'"
            }
        }
    }
}

/**
 * MySQL column type.
 * ToString returns appropriate string representation of ColumnType
 */
enum class DbColumnType {
    DbInt,
    DbString;

    override fun toString() : String {
        when (this) {
            DbInt -> {
                return "int"
            }
            //TODO take a closer look to length of String
            DbString -> {
                return "varchar(255)"
            }
        }
    }
}

/**
 * Header for MySQL DB
 */
data class DbTableHeader(val columnsHeader : List<DbColumnHeader>)

/**
 * Header for MySQL Column
 */
data class DbColumnHeader(val name : String, val type : DbColumnType) {
    /** Returns string definition of column -- name type **/
    fun toDefString() : String {
        return name + " " + type
    }
}
