package tanvd.audit.model.internal

import tanvd.audit.model.external.presenters.VersionPresenter
import tanvd.audit.model.external.records.AuditRecord
import tanvd.audit.model.external.records.InformationObject
import tanvd.audit.model.external.records.ObjectState
import tanvd.audit.model.external.types.objects.ObjectType

/**
 * Internal representation of audit record. Used to transfer objects array to DAO.
 */
internal data class AuditRecordInternal(val objects: List<Pair<ObjectType<*>, ObjectState>>,
                                        val information: MutableSet<InformationObject>) {
    var generation = 1

    companion object Factory {
        fun createFromRecordWithNewVersion(auditRecord: AuditRecord): AuditRecordInternal {
            return AuditRecordInternal(
                    auditRecord.objects.mapNotNull {
                        if (it != null)
                            it.type to it.state
                        else
                            null
                    },
                    auditRecord.informations.map {
                        if (it.type.presenter.name == VersionPresenter.name)
                            InformationObject((it.value as Long) + 1, VersionPresenter)
                        else
                            it
                    }.toMutableSet())
        }
    }
}

