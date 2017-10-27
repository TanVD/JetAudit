package tanvd.aorm.model

import java.util.*

interface Engine {
    fun toSqlDef(): String
}

class MergeTree(val dateColumn: Column<Date>, val primaryKey: List<Column<*>>, val indexGranularity: Long = 8192) : Engine {
    override fun toSqlDef(): String {
        return "MergeTree(${dateColumn.name}, (${primaryKey.joinToString { it.name }}), $indexGranularity)"
    }
}