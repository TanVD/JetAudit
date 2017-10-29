package utils

import tanvd.audit.model.external.records.ObjectState
import tanvd.audit.model.external.types.objects.*

data class TestClassLong(val hash: Long)

object TestClassLongPresenter : ObjectPresenter<TestClassLong>() {
    override val useDeserialization: Boolean = true

    override val entityName: String = "TestClassLong"

    val id = long("Id")

    override val fieldSerializers: Map<StateType<*>, (TestClassLong) -> Long> =
            hashMapOf(id to { (hash) -> hash })

    override val deserializer: (ObjectState) -> TestClassLong? = { (stateList) ->
        if (stateList[id] == null) null else TestClassLong(stateList[id]!! as Long)
    }
}

data class TestClassString(val hash: String)

object TestClassStringPresenter : ObjectPresenter<TestClassString>() {
    override val useDeserialization: Boolean = true

    override val entityName: String = "TestClassString"

    val id = string("Id")

    override val fieldSerializers: Map<StateType<*>, (TestClassString) -> String> =
            hashMapOf(id to { (hash) -> hash })

    override val deserializer: (ObjectState) -> TestClassString? = { (stateList) ->
        if (stateList[id] == null) null else TestClassString(stateList[id] as String)
    }
}

data class TestClassBoolean(val hash: Boolean)

object TestClassBooleanPresenter : ObjectPresenter<TestClassBoolean>() {
    override val useDeserialization: Boolean = true

    override val entityName: String = "TestClassBoolean"

    val id = boolean("Id")

    override val fieldSerializers: Map<StateType<*>, (TestClassBoolean) -> Boolean> =
            hashMapOf(id to { (hash) -> hash })

    override val deserializer: (ObjectState) -> TestClassBoolean? = { (stateList) ->
        if (stateList[id] == null) null else TestClassBoolean(stateList[id] as Boolean)
    }
}
