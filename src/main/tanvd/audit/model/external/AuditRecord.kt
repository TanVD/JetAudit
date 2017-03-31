package tanvd.audit.model.external

data class AuditObject(val type: AuditType<Any>, val string: String, val obj: Any)

data class AuditRecord(val objects: List<AuditObject>)
