package tanvd.audit.model.internal

import tanvd.audit.model.external.AuditType

internal data class AuditRecord(val objects: List<Pair<AuditType<Any>, String>>, val unixTimeStamp: Int)
