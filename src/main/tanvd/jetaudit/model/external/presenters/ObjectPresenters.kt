package tanvd.jetaudit.model.external.presenters

import tanvd.jetaudit.model.external.records.ObjectState
import tanvd.jetaudit.model.external.types.objects.ObjectPresenter
import tanvd.jetaudit.model.external.types.objects.long
import tanvd.jetaudit.model.external.types.objects.string

object IntPresenter : ObjectPresenter<Int>() {
    override val useDeserialization: Boolean = true

    override val entityName: String = "Int"

    val value = long("Value") { it.toLong() }

    override val deserializer: (ObjectState) -> Int? = { (stateList) -> (stateList[value] as Long?)?.toInt() }
}

object LongPresenter : ObjectPresenter<Long>() {
    override val useDeserialization: Boolean = true

    override val entityName: String = "Long"

    val value = long("Value") { it }

    override val deserializer: (ObjectState) -> Long? = { (stateList) -> stateList[value] as Long? }
}

object StringPresenter : ObjectPresenter<String>() {
    override val useDeserialization: Boolean = true

    override val entityName: String = "String"

    val value = string("Value") { it }

    override val deserializer: (ObjectState) -> String? = { (stateList) -> stateList[value] as String? }
}