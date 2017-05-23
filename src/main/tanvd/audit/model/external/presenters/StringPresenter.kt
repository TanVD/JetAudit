package tanvd.audit.model.external.presenters

import tanvd.audit.model.external.records.ObjectState
import tanvd.audit.model.external.types.objects.ObjectPresenter
import tanvd.audit.model.external.types.objects.StateStringType
import tanvd.audit.model.external.types.objects.StateType

object StringPresenter : ObjectPresenter<String>() {
    override val useDeserialization: Boolean = true

    override val entityName: String = "String"

    val value = StateStringType("Value", entityName)

    override val fieldSerializers: Map<StateType<*>, (String) -> String> =
            hashMapOf(value to { value -> value })

    override val deserializer: (ObjectState) -> String? = { (stateList) -> stateList[value] }
}