package tanvd.audit.model.internal

import tanvd.audit.model.external.presenters.VersionPresenter
import tanvd.audit.model.external.records.AuditRecord
import tanvd.audit.model.external.records.InformationObject
import tanvd.audit.model.external.types.AuditType

/**
 * Internal representation of audit record. Used to transfer objects array to DAO.
 */
internal data class AuditRecordInternal(val objects: List<Pair<AuditType<Any>, String>>,
                                        val information: Set<InformationObject>) {
    var generation = 1

    companion object Factory {
        fun createFromRecordWithNewVersion(auditRecord: AuditRecord): AuditRecordInternal {
            return AuditRecordInternal(
                    auditRecord.objects.mapNotNull {
                        if (it != null)
                            it.type to (it.type.serialize(it.obj))
                        else
                            null
                    },
                    auditRecord.information.map {
                        if (it.type.presenter.name == VersionPresenter.name)
                            InformationObject((it.value as Long) + 1, VersionPresenter)
                        else
                            it
                    }.toSet())
        }
    }

    constructor(vararg objects: Any, information: Set<InformationObject>) :
            this(objects.map { o -> AuditType.resolveType(o::class).let { it to it.serialize(o) } }, information)
}

