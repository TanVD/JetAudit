package tanvd.audit.model.external.types.objects

import tanvd.audit.exceptions.UnknownObjectTypeException
import kotlin.reflect.KClass
import kotlin.reflect.full.superclasses

data class ObjectType<T : Any>(val klass: KClass<T>, val objectPresenter: ObjectPresenter<T>) :
        ObjectSerializer<T> by objectPresenter {

    val state = objectPresenter.fieldSerializers.keys

    val entityName = objectPresenter.entityName

    companion object TypesResolution {
        private val types: MutableSet<ObjectType<Any>> = HashSet()
        private val typesByClass: MutableMap<KClass<*>, ObjectType<Any>> = HashMap()
        private val typesByEntityName: MutableMap<String, ObjectType<Any>> = HashMap()

        /**
         * Resolve KClass to ObjectType
         *
         * @throws UnknownObjectTypeException
         */
        fun resolveType(klass: KClass<*>): ObjectType<Any> {
            return _resolveType(klass) ?:
                    throw UnknownObjectTypeException("Unknown ObjectType requested to resolve by klass -- ${klass.qualifiedName}")
        }

        private fun _resolveType(klass: KClass<*>): ObjectType<Any>? {
            return typesByClass[klass] ?: klass.superclasses
                    .map { _resolveType(it) }
                    .firstOrNull { it != null }
        }

        /**
         * Resolve stateName to ObjectType
         *
         * @throws UnknownObjectTypeException
         */
        fun resolveType(name: String): ObjectType<Any> {
            return typesByEntityName[name] ?:
                    throw UnknownObjectTypeException("Unknown ObjectType requested to resolve by name -- $name")
        }

        @Synchronized
        internal fun addType(type: ObjectType<Any>) {
            types.add(type)
            typesByClass.put(type.klass, type)
            typesByEntityName.put(type.entityName, type)
        }

        internal fun getTypes(): Set<ObjectType<Any>> {
            return ObjectType.TypesResolution.types
        }

        @Synchronized
        internal fun clearTypes() {
            types.clear()
            typesByClass.clear()
            typesByEntityName.clear()
        }
    }
}
