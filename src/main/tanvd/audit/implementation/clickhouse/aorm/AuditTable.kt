package tanvd.audit.implementation.clickhouse.aorm

import org.jetbrains.annotations.TestOnly
import org.joda.time.DateTime
import tanvd.aorm.Database
import tanvd.aorm.DbType
import tanvd.aorm.Engine
import tanvd.aorm.Table
import tanvd.aorm.expression.Column
import tanvd.aorm.expression.default
import tanvd.audit.utils.PropertyLoader
import tanvd.audit.utils.RandomGenerator
import javax.sql.DataSource

object AuditTable {
    private var initialized: Boolean = false
    private lateinit var instance: AuditTableImpl

    fun init(dataSource: DataSource) {
        if (!initialized) {
            instance = AuditTableImpl("default", dataSource)
            initialized = true
        }
    }

    private fun get(): AuditTableImpl {
        if (!initialized) {
            error("AuditTable was not initialized with suitable DataSource.")
        }
        return instance
    }

    operator fun invoke(): AuditTableImpl = get()

    class AuditTableImpl(dbName: String, dataSource: DataSource) : Table(PropertyLoader["AuditTable"] ?: "AuditTable",
            Database(dbName, dataSource)) {
        val useDDL: Boolean by lazy { PropertyLoader["UseDefaultDDL"]?.toBoolean() ?: true }

        val useIsDeleted by lazy { PropertyLoader["UseIsDeleted"]?.toBoolean() ?: true }

        val date = date("DateColumn").default { DateTime.now().toDate() }

        val id = long("IdColumn").default { RandomGenerator.next() }
        val timestamp = long("TimeStampColumn").default { DateTime.now().millis }
        val version = ulong("VersionColumn").default { 0 }
        val isDeleted by lazy { boolean("IsDeletedColumn").default { false } }

        val description = arrayString("Description")

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

        override val engine: Engine = Engine.ReplacingMergeTree(date, listOf(id, timestamp), version, 512)
    }
}