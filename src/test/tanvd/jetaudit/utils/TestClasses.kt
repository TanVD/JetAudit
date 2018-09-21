package tanvd.jetaudit.utils

import tanvd.jetaudit.model.external.records.ObjectState
import tanvd.jetaudit.model.external.types.objects.ObjectPresenter
import tanvd.jetaudit.model.external.types.objects.boolean
import tanvd.jetaudit.model.external.types.objects.long
import tanvd.jetaudit.model.external.types.objects.string

data class TestClassLong(val hash: Long)

object TestClassLongPresenter : ObjectPresenter<TestClassLong>() {
    override val useDeserialization: Boolean = true

    override val entityName: String = "TestClassLong"

    val id = long("Id") { it.hash }

    override val deserializer: (ObjectState) -> TestClassLong? = { (stateList) ->
        if (stateList[id] == null) null else TestClassLong(stateList[id]!! as Long)
    }
}

data class TestClassString(val hash: String)

object TestClassStringPresenter : ObjectPresenter<TestClassString>() {
    override val useDeserialization: Boolean = true

    override val entityName: String = "TestClassString"

    val id = string("Id") { it.hash }

    override val deserializer: (ObjectState) -> TestClassString? = { (stateList) ->
        if (stateList[id] == null) null else TestClassString(stateList[id] as String)
    }
}

data class TestClassBoolean(val hash: Boolean)

object TestClassBooleanPresenter : ObjectPresenter<TestClassBoolean>() {
    override val useDeserialization: Boolean = true

    override val entityName: String = "TestClassBoolean"

    val id = boolean("Id") { it.hash }

    override val deserializer: (ObjectState) -> TestClassBoolean? = { (stateList) ->
        if (stateList[id] == null) null else TestClassBoolean(stateList[id] as Boolean)
    }
}
