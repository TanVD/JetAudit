package tanvd.audit.model.external.types

import tanvd.audit.exceptions.UnknownInformationTypeException


data class InformationType<T>(val presenter: InformationPresenter<T>, val code: String, val type: InformationInnerType) {

    enum class InformationInnerType {
        Long,
        ULong,
        String,
        Boolean;
    }

    companion object TypesResolution {
        private val informationTypes: MutableSet<InformationType<Any>> = LinkedHashSet()
        private val informationTypesByCode: MutableMap<String, InformationType<Any>> = HashMap()
        private val informationTypesByPresenter: MutableMap<InformationPresenter<*>, InformationType<Any>> = HashMap()

        /**
         * Resolve code to InformationType
         *
         * @throws UnknownInformationTypeException
         */
        fun resolveType(code: String): InformationType<Any> {
            val informationType = informationTypesByCode[code]
            if (informationType == null) {
                throw UnknownInformationTypeException("Unknown InformationType requested to resolve by code -- $code")
            } else {
                return informationType
            }
        }

        /**
         * Resolve presenter to InformationType
         *
         * @throws UnknownInformationTypeException
         */
        fun resolveType(presenter: InformationPresenter<*>): InformationType<Any> {
            val informationType = informationTypesByPresenter[presenter]
            if (informationType == null) {
                throw UnknownInformationTypeException("Unknown InformationType requested to resolve by presenter -- ${presenter.name}")
            } else {
                return informationType
            }
        }

        internal fun addType(type: InformationType<Any>) {
            informationTypes.add(type)
            informationTypesByCode.put(type.code, type)
            informationTypesByPresenter.put(type.presenter, type)
        }

        internal fun getTypes(): Set<InformationType<Any>> {
            return informationTypes
        }

        internal fun clearTypes() {
            informationTypes.clear()
            informationTypesByCode.clear()
            informationTypesByPresenter.clear()
        }
    }
}