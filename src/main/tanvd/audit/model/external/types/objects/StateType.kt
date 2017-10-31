package tanvd.audit.model.external.types.objects

import tanvd.aorm.*

data class StateType<T : Any>(val stateName: String, val objectName: String, val type: DbArrayType<T>) {
    constructor(stateName: String, objectName: String, type: DbPrimitiveType<T>) : this(stateName, objectName, type.toArray())

    internal val column : Column<List<T>, DbArrayType<T>> = Column("${objectName}_$stateName", this.type, { emptyList() })
}