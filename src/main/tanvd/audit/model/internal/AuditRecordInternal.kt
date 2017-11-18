package tanvd.audit.model.internal

import tanvd.audit.model.external.presenters.VersionType
import tanvd.audit.model.external.records.AuditRecord
import tanvd.audit.model.external.records.InformationObject
import tanvd.audit.model.external.records.ObjectState
import tanvd.audit.model.external.types.objects.ObjectType

/**
 * Internal representation of audit record. Used to transfer objects array to DAO.
 */
internal data class AuditRecordInternal(val objects: List<Pair<ObjectType<*>, ObjectState>>,
                                        val information: LinkedHashSet<InformationObject<*>>) {
    var generation = 1

    companion object {
        fun createFromRecordWithNewVersion(auditRecord: AuditRecord): AuditRecordInternal {
            return AuditRecordInternal(
                    auditRecord.objects.map {
                        it.type to it.state
                    },
                    LinkedHashSet(auditRecord.informations.map {
                        if (it.type == VersionType)
                            InformationObject((it.value as Long) + 1, VersionType)
                        else
                            it
                    }))
        }
    }
}

