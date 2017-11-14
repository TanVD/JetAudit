package tanvd.audit.model.external.types

import tanvd.aorm.Column
import tanvd.aorm.DbType

abstract class ColumnWrapper<T: Any, out E : DbType<T>>(val column: Column<T, E>)