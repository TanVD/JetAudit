package tanvd.audit.model.external.records

import tanvd.audit.model.external.types.AuditType
import tanvd.audit.model.external.types.InformationPresenter

/**
 * External representation of loaded audit object with rich type informations.
 */
data class AuditObject(val type: AuditType<Any>, val obj: Any)

/**
 * External representation of loaded audit record
 */
data class AuditRecord(val objects: List<AuditObject?>, val informations: MutableSet<InformationObject>) {
    fun <T> getInformationValue(presenter: InformationPresenter<T>): T? {
        @Suppress("UNCHECKED_CAST")
        return informations.find {
            it.type.presenter.name == presenter.name
        }?.value as T?
    }
}
