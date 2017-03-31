package tanvd.audit.model.internal

import tanvd.audit.model.external.AuditType

internal data class AuditRecordInternal(val objects: List<Pair<AuditType<Any>, String>>, val unixTimeStamp: Long)
