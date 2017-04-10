package tanvd.audit.model.external

/**
 * External representation of loaded audit object with rich type information.
 */
data class AuditObject(val type: AuditType<Any>, val string: String, val obj: Any)

/**
 * External representation of loaded audit record
 */
data class AuditRecord(val objects: List<AuditObject>, val unixTimeStamp: Long)
