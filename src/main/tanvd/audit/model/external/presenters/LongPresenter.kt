package tanvd.audit.model.external.presenters

import tanvd.aorm.DbLong
import tanvd.audit.model.external.records.ObjectState
import tanvd.audit.model.external.types.objects.ObjectPresenter
import tanvd.audit.model.external.types.objects.StateType

object LongPresenter : ObjectPresenter<Long>() {
    override val useDeserialization: Boolean = true

    override val entityName: String = "Long"

    val value = StateType("Value", entityName, DbLong())

    override val fieldSerializers: Map<StateType<*>, (Long) -> Long> =
            hashMapOf(value to { value -> value })

    override val deserializer: (ObjectState) -> Long? = { (stateList) -> stateList[value] as Long? }
}