package tanvd.audit.model

class AuditRecord(val objects: List<Pair<AuditType<Any>, String>>)
