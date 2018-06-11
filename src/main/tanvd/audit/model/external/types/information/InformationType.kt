package tanvd.audit.model.external.types.information

import org.jetbrains.annotations.TestOnly
import tanvd.aorm.DbPrimitiveType
import tanvd.aorm.expression.Column
import tanvd.audit.exceptions.UnknownInformationTypeException
import tanvd.audit.implementation.clickhouse.aorm.AuditTable
import tanvd.audit.model.external.types.ColumnWrapper

abstract class InformationType<T : Any>(val code: String, val type: DbPrimitiveType<T>, val default: () -> T)
    : ColumnWrapper<T, DbPrimitiveType<T>>() {
    constructor(column: Column<T, DbPrimitiveType<T>>) : this(column.name, column.type, column.defaultFunction!!)

    override val column: Column<T, DbPrimitiveType<T>> by lazy { Column(code, type, AuditTable, default) }


    companion object TypesResolution {
        private val informationTypesByCode: MutableMap<String, InformationType<Any>> = LinkedHashMap()

        /**
         * Resolve stateName to InformationType
         *
         * @throws UnknownInformationTypeException
         */
        @Suppress("UNCHECKED_CAST")
        fun <T : Any> resolveType(code: String): InformationType<T> {
            return informationTypesByCode[code] as? InformationType<T>
                    ?: throw UnknownInformationTypeException("Unknown InformationType requested to resolve by stateName -- $code")
        }

        @Suppress("UNCHECKED_CAST")
        @Synchronized
        internal fun addType(type: InformationType<*>) {
            informationTypesByCode[type.code] = type as InformationType<Any>
        }

        internal fun getTypes(): Set<InformationType<Any>> = informationTypesByCode.values.toSet()

        @TestOnly
        @Synchronized
        internal fun clearTypes() {
            informationTypesByCode.clear()
        }
    }

    /** Equals by code.  **/
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as InformationType<*>

        if (code != other.code) return false

        return true
    }

    override fun hashCode(): Int = code.hashCode()
}