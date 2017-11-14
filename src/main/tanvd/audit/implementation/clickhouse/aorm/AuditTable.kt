package tanvd.audit.implementation.clickhouse.aorm

import org.jetbrains.annotations.TestOnly
import org.joda.time.DateTime
import tanvd.aorm.*
import tanvd.audit.utils.PropertyLoader
import tanvd.audit.utils.RandomGenerator

object AuditTable : Table("AuditTable", AuditDatabase) {
    override val useDDL: Boolean by lazy { PropertyLoader["UseDefaultDDL"]?.toBoolean() ?: true }
    val useIsDeleted by lazy { PropertyLoader["UseIsDeleted"]?.toBoolean() ?: true }

    val date = date("DateColumn").default { DateTime.now().toDate() }

    val id = long("IdColumn").default { RandomGenerator.next() }
    val timestamp = long("TimeStampColumn").default { DateTime.now().millis }
    val version = ulong("VersionColumn").default { 0 }
    val isDeleted by lazy { boolean("IsDeletedColumn").default { false } }

    val description = arrayString("DescriptionColumn")

    @TestOnly
    @Suppress("UNCHECKED_CAST")
    internal fun resetColumns() {
        columns.clear()
        columns.add(date as Column<Any, DbType<Any>>)
        columns.add(id as Column<Any, DbType<Any>>)
        columns.add(timestamp as Column<Any, DbType<Any>>)
        columns.add(version as Column<Any, DbType<Any>>)
        columns.add(isDeleted as Column<Any, DbType<Any>>)
        columns.add(description as Column<Any, DbType<Any>>)
    }

    override val engine: Engine = ReplacingMergeTree(date, listOf(id, timestamp), version, 512)
}