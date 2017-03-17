package tanvd.audit.model

import java.util.*
import kotlin.reflect.KClass

data class AuditType<T>(val klass : KClass<*>, val code : String,
                        val serializer : AuditSerializer<T>) {
    companion object TypesResolution {
        val auditTypesByClass : MutableMap<KClass<*>, AuditType<Any>> = HashMap()
        val auditTypesByCode : MutableMap<String, AuditType<Any>> = HashMap()

        /**
         * Resolve your KClass to AuditType
         */
        fun resolveType(klass : KClass<*>) : AuditType<Any> {
            return auditTypesByClass[klass]!!
        }

        fun resolveType(code : String) : AuditType<Any> {
            return auditTypesByCode[code]!!
        }

        fun addType(type : AuditType<Any>) {
            auditTypesByClass.put(type.klass, type)
            auditTypesByCode.put(type.code, type)
        }
    }
}
