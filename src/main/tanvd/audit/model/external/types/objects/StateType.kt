package tanvd.audit.model.external.types.objects

import tanvd.aorm.DbArrayType
import tanvd.aorm.DbPrimitiveType
import tanvd.aorm.expression.Column
import tanvd.audit.implementation.clickhouse.aorm.AuditTable
import tanvd.audit.model.external.types.ColumnWrapper

data class StateType<T : Any>(val stateName: String, val objectName: String, val type: DbArrayType<T>) :
        ColumnWrapper<List<T>, DbArrayType<T>>() {
    constructor(stateName: String, objectName: String, type: DbPrimitiveType<T>) : this(stateName, objectName, type.toArray())

    override val column: Column<List<T>, DbArrayType<T>> by lazy { Column("${objectName}_$stateName", type, AuditTable(), { emptyList() }) }
}