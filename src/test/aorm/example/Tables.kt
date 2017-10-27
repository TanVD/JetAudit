package aorm.example

import org.joda.time.DateTime
import tanvd.aorm.model.*
import java.util.*

object AuditTable : Table("Audit", {AuditRecord(it)}) {
    val date = date("date_column").default { DateTime.now().toDate() }
    val id = long("id_column").default { Random().nextLong() }
    val version = ulong("version_column")

    override val engine = MergeTree(date, listOf(date, id))
}

class AuditRecord(row: Row) : Entity(row) {
    val date by AuditTable.date
    val id by AuditTable.id
    val version by AuditTable.version
}