package tanvd.audit.model.external.presenters

import tanvd.aorm.DbLong
import tanvd.audit.model.external.records.ObjectState
import tanvd.audit.model.external.types.objects.ObjectPresenter
import tanvd.audit.model.external.types.objects.StateType

object IntPresenter : ObjectPresenter<Int>() {
    override val useDeserialization: Boolean = true

    override val entityName: String = "Int"

    val value = StateType("Value", entityName, DbLong())

    override val fieldSerializers: Map<StateType<*>, (Int) -> String> =
            hashMapOf(value to { value -> value.toString() })

    override val deserializer: (ObjectState) -> Int? = { (stateList) -> stateList[value] as Int? }
}