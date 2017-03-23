package tanvd.audit.implementation.dao

import tanvd.audit.model.AuditRecord
import tanvd.audit.model.AuditType

interface AuditDao {
    fun saveRecord(auditRecord: AuditRecord)

    fun saveRecords(auditRecords: List<AuditRecord>)

    fun <T> addTypeInDbModel(type : AuditType<T>)

    fun <T> loadRecords(type : AuditType<T>, id : String) : List<AuditRecord>

    fun dropTable(tableName : String)
}