package tanvd.audit.model.external.records

import tanvd.audit.model.external.types.information.InformationType

data class InformationObject<out T : Any>(val value: T, val type: InformationType<out T>)