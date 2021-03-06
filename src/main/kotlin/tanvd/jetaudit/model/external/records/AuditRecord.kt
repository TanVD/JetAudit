package tanvd.jetaudit.model.external.records

import tanvd.jetaudit.model.external.types.information.InformationType

/**
 * External representation of loaded audit record
 */
data class AuditRecord(val objects: List<AuditObject>, val informations: Set<InformationObject<*>>) {
    fun <T : Any> getInformationValue(presenter: InformationType<T>): T? {
        @Suppress("UNCHECKED_CAST")
        return informations.find {
            it.type.code == presenter.code
        }?.value as T?
    }
}
