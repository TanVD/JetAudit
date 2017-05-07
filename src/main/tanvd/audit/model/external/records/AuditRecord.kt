package tanvd.audit.model.external.records

import tanvd.audit.model.external.types.information.InformationPresenter
import tanvd.audit.model.external.types.objects.ObjectType
import tanvd.audit.model.external.types.objects.StateType

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
