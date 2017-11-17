package tanvd.audit.model.external.types

import tanvd.aorm.DbType
import tanvd.aorm.expression.Column

abstract class ColumnWrapper<T: Any, out E : DbType<T>> {
    abstract val column: Column<T, E>
}