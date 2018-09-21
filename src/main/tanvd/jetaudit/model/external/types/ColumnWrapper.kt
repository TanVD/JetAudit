package tanvd.jetaudit.model.external.types

import tanvd.aorm.DbType
import tanvd.aorm.expression.Column

abstract class ColumnWrapper<T : Any, out E : DbType<T>> {
    /**
     * To solve problem with order of init of objects (AuditTable may be initialized
     * later than some of presenters) we use abstract val for column and create in all
     * children it as lazy value
     */
    abstract val column: Column<T, E>
}