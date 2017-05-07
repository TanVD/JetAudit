package tanvd.audit.model.external.presenters

object IntPresenter : tanvd.audit.model.external.types.objects.ObjectPresenter<Int>() {
    override val entityName: String = "Int"

    val value = tanvd.audit.model.external.types.objects.StateLongType<Int>("Value", entityName)

    override val fieldSerializers: Map<tanvd.audit.model.external.types.objects.StateType<Int>, (Int) -> String> =
            hashMapOf(tanvd.audit.model.external.presenters.IntPresenter.value to { value -> value.toString()})

    override val deserializer: (tanvd.audit.model.external.records.ObjectState) -> Int? = { (stateList) -> stateList[tanvd.audit.model.external.presenters.IntPresenter.value]?.toInt() }
}