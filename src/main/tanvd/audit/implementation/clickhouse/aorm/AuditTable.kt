package tanvd.audit.implementation.clickhouse.aorm

import org.joda.time.DateTime
import tanvd.aorm.Engine
import tanvd.aorm.MergeTree
import tanvd.aorm.Table
import tanvd.aorm.default
import tanvd.audit.utils.RandomGenerator

object AuditTable : Table("AuditTable") {
    val date = date("DateColumn").default { DateTime.now().toDate() }

    val id = long("IdColumn").default { RandomGenerator.next() }
    val timestamp = long("TimeStampColumn").default { DateTime.now().millis }
    val version = long("VersionColumn").default { 0 }
    val isDeleted = boolean("IsDeletedColumn").default { false }

    val description = arrayString("DescriptionColumn")

    override val engine: Engine = MergeTree(date, listOf(id, timestamp), 512)
}