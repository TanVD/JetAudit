package tanvd.audit.model.external.records

import tanvd.audit.model.external.types.information.InformationPresenter
import tanvd.audit.model.external.types.information.InformationType

data class InformationObject(val value: Any, val type: InformationType<Any>) {
    constructor(value: Any, presenter: InformationPresenter<*>) : this(value, InformationType.resolveType(presenter))
}