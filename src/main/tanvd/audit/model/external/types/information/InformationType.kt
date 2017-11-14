package tanvd.audit.model.external.types.information

import tanvd.aorm.Column
import tanvd.aorm.DbPrimitiveType
import tanvd.audit.exceptions.UnknownInformationTypeException
import tanvd.audit.model.external.types.ColumnWrapper

abstract class InformationType<T : Any>(val code: String, val type: DbPrimitiveType<T>, val default: () -> T,
                                        column: Column<T, DbPrimitiveType<T>> = Column(code, type, default))
    : ColumnWrapper<T, DbPrimitiveType<T>>(column){
    constructor(column: Column<T, DbPrimitiveType<T>>) : this(column.name, column.type, column.defaultFunction!!)

    companion object TypesResolution {
        private val informationTypes: MutableSet<InformationType<Any>> = LinkedHashSet()
        private val informationTypesByCode: MutableMap<String, InformationType<Any>> = HashMap()

        /**
         * Resolve stateName to InformationType
         *
         * @throws UnknownInformationTypeException
         */
        @Suppress("UNCHECKED_CAST")
        fun <T : Any> resolveType(code: String): InformationType<T> {
            return informationTypesByCode[code] as? InformationType<T> ?:
                    throw UnknownInformationTypeException("Unknown InformationType requested to resolve by stateName -- $code")
        }

        @Suppress("UNCHECKED_CAST")
        @Synchronized
        internal fun addType(type: InformationType<*>) {
            informationTypes.add(type as InformationType<Any>)
            informationTypesByCode.put(type.code, type)
        }

        internal fun getTypes(): Set<InformationType<Any>> {
            return informationTypes
        }

        @Synchronized
        internal fun clearTypes() {
            informationTypes.clear()
            informationTypesByCode.clear()
        }
    }
}