package tanvd.audit.model.external.presenters

import tanvd.audit.model.external.records.ObjectState
import tanvd.audit.model.external.types.objects.ObjectPresenter
import tanvd.audit.model.external.types.objects.string

object StringPresenter : ObjectPresenter<String>() {
    override val useDeserialization: Boolean = true

    override val entityName: String = "String"

    val value = string("Value", { it })

    override val deserializer: (ObjectState) -> String? = { (stateList) -> stateList[value] as String? }
}