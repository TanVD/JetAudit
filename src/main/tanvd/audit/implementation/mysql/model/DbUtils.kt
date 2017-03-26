package tanvd.audit.implementation.mysql.model


/**
 * Row of MySQL DB.
 */
internal data class DbRow(val columns: List<DbColumn>) {
    constructor(vararg columns: DbColumn) : this(columns.toList())

    fun toStringHeader(): String {
        return columns.map { it.name }.joinToString()
    }

    fun toPlaceholders(): String {
        return columns.map { "?" }.joinToString()
    }
}

/**
 * Column of MySQL DB.
 */
internal data class DbColumn(val name: String, val element: String, val type: DbColumnType) {
    constructor(header: DbColumnHeader, element: String) : this(header.name, element, header.type)
}

/**
 * MySQL column type.
 * ToString returns appropriate string representation of ColumnType
 */
internal enum class DbColumnType {
    DbInt,
    DbString;

    override fun toString(): String {
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
internal data class DbTableHeader(val columnsHeader: List<DbColumnHeader>) {
    constructor(vararg columnsHeader: DbColumnHeader) : this(columnsHeader.toList())
}

/**
 * Header for MySQL Column
 */
internal data class DbColumnHeader(val name: String, val type: DbColumnType) {
    /** Returns string definition of column -- name type **/
    fun toDefString(): String {
        return name + " " + type
    }
}
