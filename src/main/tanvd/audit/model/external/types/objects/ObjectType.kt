package tanvd.audit.model.external.types.objects

import tanvd.audit.exceptions.UnknownObjectTypeException
import kotlin.reflect.KClass

data class ObjectType<T : Any>(val klass: KClass<T>, val entityName: String,
                               val serializer: ObjectSerializer<T>, val state: Set<StateType<T>>) :
        ObjectSerializer<T> by serializer {

    constructor(klass: KClass<T>, objectPresenter: ObjectPresenter<T>) : this(klass, objectPresenter.entityName,
            objectPresenter, objectPresenter.fieldSerializers.keys)

    companion object TypesResolution {
        private val types: MutableSet<ObjectType<Any>> = java.util.HashSet()
        private val typesByClass: MutableMap<KClass<*>, ObjectType<Any>> = HashMap()
        private val typesByEntityName: MutableMap<String, ObjectType<Any>> = HashMap()

        /**
         * Resolve KClass to ObjectType
         *
         * @throws UnknownObjectTypeException
         */
        fun resolveType(klass: KClass<*>): ObjectType<Any> {
            val auditType = ObjectType.TypesResolution.typesByClass[klass]
            if (auditType == null) {
                throw UnknownObjectTypeException("Unknown ObjectType requested to resolve by klass --" +
                        " ${klass.qualifiedName}")
            } else {
                return auditType
            }
        }

        /**
         * Resolve stateName to ObjectType
         *
         * @throws UnknownObjectTypeException
         */
        fun resolveType(name: String): ObjectType<Any> {
            val auditType = typesByEntityName[name]
            if (auditType == null) {
                throw UnknownObjectTypeException("Unknown ObjectType requested to resolve by name -- $name")
            } else {
                return auditType
            }
        }

        internal fun addType(type: ObjectType<Any>) {
            types.add(type)
            typesByClass.put(type.klass, type)
            typesByEntityName.put(type.entityName, type)
        }

        internal fun getTypes(): Set<ObjectType<Any>> {
            return ObjectType.TypesResolution.types
        }

        internal fun clearTypes() {
            types.clear()
            typesByClass.clear()
            typesByEntityName.clear()
        }
    }
}
