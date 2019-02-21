package tanvd.jetaudit.implementation.clickhouse.aorm

import org.jetbrains.annotations.TestOnly
import org.joda.time.DateTime
import tanvd.aorm.DbType
import tanvd.aorm.Engine
import tanvd.aorm.Table
import tanvd.aorm.expression.Column
import tanvd.aorm.expression.default
import tanvd.jetaudit.utils.Conf
import tanvd.jetaudit.utils.PropertyLoader
import tanvd.jetaudit.utils.RandomGenerator

object AuditTable : Table(PropertyLoader[Conf.AUDIT_TABLE]) {
    val useDDL: Boolean by lazy { PropertyLoader[Conf.DEFAULT_DDL].toBoolean() }

    val date = date("DateColumn").default { DateTime.now().toDate() }

    val id = int64("IdColumn").default { RandomGenerator.next() }
    val timestamp = int64("TimeStampColumn").default { DateTime.now().millis }
    val version = uint64("VersionColumn").default { 0 }
    val isDeleted = boolean("IsDeletedColumn").default { false }

    val description = arrayString("Description")

    @TestOnly
    @Suppress("UNCHECKED_CAST")
    internal fun resetColumns() {
        columns.clear()
        columns.add(date as Column<Any, DbType<Any>>)
        columns.add(id as Column<Any, DbType<Any>>)
        columns.add(timestamp as Column<Any, DbType<Any>>)
        columns.add(version as Column<Any, DbType<Any>>)
        columns.add(description as Column<Any, DbType<Any>>)
        columns.add(isDeleted as Column<Any, DbType<Any>>)
    }

    override val engine: Engine = Engine.ReplacingMergeTree(date, listOf(id, timestamp), version, 512)
}