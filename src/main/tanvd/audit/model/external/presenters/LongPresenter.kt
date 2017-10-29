package tanvd.audit.model.external.presenters

import tanvd.audit.model.external.records.ObjectState
import tanvd.audit.model.external.types.objects.ObjectPresenter
import tanvd.audit.model.external.types.objects.long

object LongPresenter : ObjectPresenter<Long>() {
    override val useDeserialization: Boolean = true

    override val entityName: String = "Long"

    val value = long("Value", { it })

    override val deserializer: (ObjectState) -> Long? = { (stateList) -> stateList[value] as Long? }
}