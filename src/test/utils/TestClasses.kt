package utils

import tanvd.audit.model.external.records.ObjectState
import tanvd.audit.model.external.types.objects.*

data class TestClassLong(val hash: Long)

object TestClassLongPresenter : ObjectPresenter<TestClassLong>() {
    override val useDeserialization: Boolean = true

    override val entityName: String = "TestClassLong"

    val id = StateLongType("Id", entityName)

    override val fieldSerializers: Map<StateType<*>, (TestClassLong) -> String> =
            hashMapOf(id to { (hash) -> hash.toString() })

    override val deserializer: (ObjectState) -> TestClassLong? = { (stateList) ->
        if (stateList[id] == null) null else TestClassLong(stateList[id]!!.toLong())
    }
}

data class TestClassString(val hash: String)

object TestClassStringPresenter : ObjectPresenter<TestClassString>() {
    override val useDeserialization: Boolean = true

    override val entityName: String = "TestClassString"

    val id = StateStringType("Id", entityName)

    override val fieldSerializers: Map<StateType<*>, (TestClassString) -> String> =
            hashMapOf(id to { (hash) -> hash })

    override val deserializer: (ObjectState) -> TestClassString? = { (stateList) ->
        if (stateList[id] == null) null else TestClassString(stateList[id]!!)
    }
}

data class TestClassBoolean(val hash: Boolean)

object TestClassBooleanPresenter : ObjectPresenter<TestClassBoolean>() {
    override val useDeserialization: Boolean = true

    override val entityName: String = "TestClassBoolean"

    val id = StateBooleanType("Id", entityName)

    override val fieldSerializers: Map<StateType<*>, (TestClassBoolean) -> String> =
            hashMapOf(id to { (hash) -> hash.toString() })

    override val deserializer: (ObjectState) -> TestClassBoolean? = { (stateList) ->
        if (stateList[id] == null) null else TestClassBoolean(stateList[id]!!.toBoolean())
    }
}
