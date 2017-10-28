package tanvd.audit.model.external.presenters

import tanvd.audit.implementation.clickhouse.aorm.AuditTable
import tanvd.audit.model.external.types.information.InformationType
import tanvd.audit.utils.RandomGenerator
import java.util.*

object TimeStampType : InformationType<Long>(AuditTable.timestamp) {
    override fun getDefault(): Long {
        return System.currentTimeMillis()
    }
}

object VersionType : InformationType<Long>(AuditTable.version) {
    override fun getDefault(): Long {
        return 0
    }
}

object IdType : InformationType<Long>(AuditTable.id) {
    override fun getDefault(): Long {
        return RandomGenerator.next()
    }
}

object DateType : InformationType<Date>(AuditTable.date) {
    override fun getDefault(): Date {
        return Date()
    }
}

object IsDeletedType : InformationType<Boolean>(AuditTable.isDeleted) {
    override fun getDefault(): Boolean {
        return false
    }
}
