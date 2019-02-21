package tanvd.jetaudit.model.external.records

import tanvd.jetaudit.model.external.types.information.InformationType

data class InformationObject<out T : Any>(val value: T, val type: InformationType<out T>)