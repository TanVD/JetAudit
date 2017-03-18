package tanvd.audit.model

import java.util.*

class AuditRecord(val objects: MutableList<Pair<AuditType<Any>, String>> = ArrayList())
