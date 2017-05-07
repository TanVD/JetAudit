package tanvd.audit.model.external.presenters

object LongPresenter : tanvd.audit.model.external.types.objects.ObjectPresenter<Long>() {
    override val entityName: String = "Long"

    val value = tanvd.audit.model.external.types.objects.StateLongType<Long>("Value", entityName)

    override val fieldSerializers: Map<tanvd.audit.model.external.types.objects.StateType<Long>, (Long) -> String> =
            hashMapOf(tanvd.audit.model.external.presenters.LongPresenter.value to { value -> value.toString()})

    override val deserializer: (tanvd.audit.model.external.records.ObjectState) -> Long? = { (stateList) -> stateList[tanvd.audit.model.external.presenters.LongPresenter.value]?.toLong() }
}