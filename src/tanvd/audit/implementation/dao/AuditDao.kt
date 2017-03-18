package tanvd.audit.implementation.dao

import tanvd.audit.model.AuditRecord
import tanvd.audit.model.AuditType

interface AuditDao {
    fun saveRow(auditRecord: AuditRecord)

    fun saveRows(auditRecords: List<AuditRecord>)

    fun <T> addType(type : AuditType<T>)

    fun <T> loadRow(type : AuditType<T>, id : String) : List<AuditRecord>

    fun dropTable(tableName : String)
}