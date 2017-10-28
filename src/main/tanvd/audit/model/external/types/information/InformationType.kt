package tanvd.audit.model.external.types.information

import tanvd.aorm.Column
import tanvd.aorm.DbPrimitiveType
import tanvd.audit.exceptions.UnknownInformationTypeException

abstract class InformationType<T : Any>(val code: String, val type : DbPrimitiveType<T>,
                                        val column: Column<T, DbPrimitiveType<T>> = Column(code, type)) {
    constructor(column: Column<T, DbPrimitiveType<T>>): this(column.name, column.type, column)

    abstract fun getDefault(): T

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
            val informationType = informationTypesByCode[code]
            if (informationType == null) {
                throw UnknownInformationTypeException("Unknown InformationType requested to resolve by stateName -- $code")
            } else {
                return informationType as InformationType<T>
            }
        }

        @Synchronized
        internal fun addType(type: InformationType<Any>) {
            informationTypes.add(type)
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