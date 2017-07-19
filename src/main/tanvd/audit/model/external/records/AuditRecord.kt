package tanvd.audit.model.external.records

import tanvd.audit.model.external.types.information.InformationPresenter

/**
 * External representation of loaded audit record
 */
data class AuditRecord(val objects: List<AuditObject>, val informations: MutableSet<InformationObject<*>>) {
    fun <T : Any> getInformationValue(presenter: InformationPresenter<T>): T? {
        @Suppress("UNCHECKED_CAST")
        return informations.find {
            it.type.code == presenter.code
        }?.value as T?
    }
}
