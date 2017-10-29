package tanvd.audit.model.external.presenters

import tanvd.audit.model.external.records.ObjectState
import tanvd.audit.model.external.types.objects.ObjectPresenter
import tanvd.audit.model.external.types.objects.long

object IntPresenter : ObjectPresenter<Int>() {
    override val useDeserialization: Boolean = true

    override val entityName: String = "Int"

    val value = long("Value", { it.toLong() })

    override val deserializer: (ObjectState) -> Int? = { (stateList) -> stateList[value] as Int? }
}