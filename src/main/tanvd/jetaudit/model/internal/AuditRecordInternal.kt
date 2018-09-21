package tanvd.jetaudit.model.internal

import tanvd.jetaudit.model.external.presenters.IsDeletedType
import tanvd.jetaudit.model.external.presenters.VersionType
import tanvd.jetaudit.model.external.records.AuditRecord
import tanvd.jetaudit.model.external.records.InformationObject
import tanvd.jetaudit.model.external.records.ObjectState
import tanvd.jetaudit.model.external.types.objects.ObjectType

/**
 * Internal representation of audit record. Used to transfer objects array to DAO.
 */
internal data class AuditRecordInternal(val objects: List<Pair<ObjectType<*>, ObjectState>>,
                                        val information: LinkedHashSet<InformationObject<*>>) {
    var generation = 1

    companion object {

        private fun makeCopyWithChangedInformation(auditRecord: AuditRecord, transform: (InformationObject<*>) -> InformationObject<*>) =
                AuditRecordInternal(
                        objects = auditRecord.objects.map { it.type to it.state },
                        information = LinkedHashSet(auditRecord.informations.map(transform))
                )

        fun createFromRecordWithNewVersion(auditRecord: AuditRecord) =
                makeCopyWithChangedInformation(auditRecord) {
                    if (it.type == VersionType)
                        it.copy(value = (it.value as Long) + 1)
                    else
                        it
                }

        fun markRecordAsDeleted(auditRecord: AuditRecord) =
                makeCopyWithChangedInformation(auditRecord) {
                    when {
                        it.type == IsDeletedType && !(it.value as Boolean) -> it.copy(value = true)
                        it.type == VersionType -> it.copy(value = it.value as Long + 1)
                        else -> it
                    }
                }
    }
}

