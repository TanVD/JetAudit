package tanvd.audit.model.external.types

import tanvd.audit.exceptions.UnknownAuditTypeException
import java.util.*
import kotlin.collections.HashMap
import kotlin.reflect.KClass

data class AuditType<T>(val klass: KClass<*>, val code: String, val serializer: AuditSerializer<T>) :
        AuditSerializer<T> by serializer {

    companion object TypesResolution {
        private val auditTypes: MutableSet<AuditType<Any>> = HashSet()
        private val auditTypesByClass: MutableMap<KClass<*>, AuditType<Any>> = HashMap()
        private val auditTypesByCode: MutableMap<String, AuditType<Any>> = HashMap()

        /**
         * Resolve KClass to AuditType
         *
         * @throws UnknownAuditTypeException
         */
        fun resolveType(klass: KClass<*>): AuditType<Any> {
            val auditType = auditTypesByClass[klass]
            if (auditType == null) {
                throw UnknownAuditTypeException("Unknown AuditType requested to resolve by klass --" +
                        " ${klass.qualifiedName}")
            } else {
                return auditType
            }
        }

        /**
         * Resolve code to AuditType
         *
         * @throws UnknownAuditTypeException
         */
        fun resolveType(code: String): AuditType<Any> {
            val auditType = auditTypesByCode[code]
            if (auditType == null) {
                throw UnknownAuditTypeException("Unknown AuditType requested to resolve by code -- $code")
            } else {
                return auditType
            }
        }

        internal fun addType(type: AuditType<Any>) {
            auditTypes.add(type)
            auditTypesByClass.put(type.klass, type)
            auditTypesByCode.put(type.code, type)
        }

        internal fun getTypes(): Set<AuditType<Any>> {
            return auditTypes
        }

        internal fun clearTypes() {
            auditTypes.clear()
            auditTypesByClass.clear()
            auditTypesByCode.clear()
        }
    }
}
