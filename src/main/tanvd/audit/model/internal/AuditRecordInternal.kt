package tanvd.audit.model.internal

import tanvd.audit.model.external.AuditType

/**
 * Internal representation of audit record. Used to transfer objects array to DAO.
 */
internal data class AuditRecordInternal(val objects: List<Pair<AuditType<Any>, String>>, val unixTimeStamp: Long) {
    var generation = 1

    constructor(vararg objects: Any, unixTimeStamp: Long) :
            this(objects.map { o -> AuditType.resolveType(o::class).let { it to it.serialize(o) } }, unixTimeStamp)
}

