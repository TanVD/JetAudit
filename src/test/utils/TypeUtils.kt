package utils

import tanvd.audit.implementation.clickhouse.AuditDaoClickhouseImpl
import tanvd.audit.model.external.AuditType

object TypeUtils {
    fun clearTypes() {
        AuditType.clearTypes()
        AuditDaoClickhouseImpl.clearTypes()
    }
}
