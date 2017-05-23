package tanvd.audit.model.external.presenters

import tanvd.audit.model.external.records.ObjectState
import tanvd.audit.model.external.types.objects.ObjectPresenter
import tanvd.audit.model.external.types.objects.StateLongType
import tanvd.audit.model.external.types.objects.StateType

object LongPresenter : ObjectPresenter<Long>() {
    override val useDeserialization: Boolean = true

    override val entityName: String = "Long"

    val value = StateLongType("Value", entityName)

    override val fieldSerializers: Map<StateType<*>, (Long) -> String> =
            hashMapOf(value to { value -> value.toString() })

    override val deserializer: (ObjectState) -> Long? = { (stateList) -> stateList[value]?.toLong() }
}