package tanvd.audit.model.external.types.objects

import tanvd.aorm.Column
import tanvd.aorm.DbArrayType
import tanvd.aorm.DbPrimitiveType
import tanvd.audit.model.external.types.ColumnWrapper

data class StateType<T : Any>(val stateName: String, val objectName: String, val type: DbArrayType<T>) :
        ColumnWrapper<List<T>, DbArrayType<T>>(Column("${objectName}_$stateName", type, { emptyList() })) {
    constructor(stateName: String, objectName: String, type: DbPrimitiveType<T>) : this(stateName, objectName, type.toArray())
}