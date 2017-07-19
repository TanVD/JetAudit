package tanvd.audit.model.external.records

import tanvd.audit.model.external.types.information.InformationPresenter
import tanvd.audit.model.external.types.information.InformationType

data class InformationObject<T : Any>(val value: T, val type: InformationType<T>) {
    constructor(value: T, presenter: InformationPresenter<T>) : this(value, InformationType.resolveType(presenter))
}