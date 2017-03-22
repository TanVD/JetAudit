package tanvd.audit.model

import java.util.*
import kotlin.reflect.KClass

data class AuditType<T>(val klass : KClass<*>, val code : String, val serializer : AuditSerializer<T>) {

    fun serialize(toSerialize : T) : String {
        return serializer.serialize(toSerialize)
    }

    fun deserialize(serialized : String) : T {
        return serializer.deserialize(serialized)
    }

    companion object TypesResolution {
        private val auditTypes : MutableList<AuditType<Any>> = ArrayList()
        private val auditTypesByClass : MutableMap<KClass<*>, AuditType<Any>> = HashMap()
        private val auditTypesByCode : MutableMap<String, AuditType<Any>> = HashMap()

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
            auditTypes.add(type)
            auditTypesByClass.put(type.klass, type)
            auditTypesByCode.put(type.code, type)
        }

        fun getTypes() : List<AuditType<Any>> {
            return auditTypes
        }

        fun clearTypes() {
            auditTypes.clear()
            auditTypesByCode.clear()
            auditTypesByCode.clear()
        }
    }
}
