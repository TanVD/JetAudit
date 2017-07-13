package tanvd.audit.model.external.types.information

import tanvd.audit.exceptions.UnknownInformationTypeException
import tanvd.audit.model.external.types.InnerType

data class InformationType<T>(val presenter: InformationPresenter<T>, val type: InnerType) :
        InformationSerializer<T> by presenter {

    val code = presenter.code

    companion object TypesResolution {
        private val informationTypes: MutableSet<InformationType<Any>> = LinkedHashSet()
        private val informationTypesByCode: MutableMap<String, InformationType<Any>> = HashMap()
        private val informationTypesByPresenter: MutableMap<InformationPresenter<*>, InformationType<Any>> = HashMap()

        /**
         * Resolve stateName to InformationType
         *
         * @throws UnknownInformationTypeException
         */
        fun resolveType(code: String): InformationType<Any> {
            val informationType = informationTypesByCode[code]
            if (informationType == null) {
                throw UnknownInformationTypeException("Unknown InformationType requested to resolve by stateName -- $code")
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
                throw UnknownInformationTypeException("Unknown InformationType requested to resolve by presenter -- ${presenter.code}")
            } else {
                return informationType
            }
        }

        @Synchronized
        internal fun addType(type: InformationType<Any>) {
            informationTypes.add(type)
            informationTypesByCode.put(type.code, type)
            informationTypesByPresenter.put(type.presenter, type)
        }

        internal fun getTypes(): Set<InformationType<Any>> {
            return informationTypes
        }

        @Synchronized
        internal fun clearTypes() {
            informationTypes.clear()
            informationTypesByCode.clear()
            informationTypesByPresenter.clear()
        }
    }
}