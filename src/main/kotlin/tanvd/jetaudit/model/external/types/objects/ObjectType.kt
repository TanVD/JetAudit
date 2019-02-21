package tanvd.jetaudit.model.external.types.objects

import org.jetbrains.annotations.TestOnly
import tanvd.jetaudit.exceptions.UnknownObjectTypeException
import kotlin.reflect.KClass
import kotlin.reflect.full.superclasses

data class ObjectType<T : Any>(val klass: KClass<T>, val objectPresenter: ObjectPresenter<T>) :
        ObjectSerializer<T> by objectPresenter {

    val state = objectPresenter.fieldSerializers.keys

    val entityName = objectPresenter.entityName

    companion object TypesResolution {
        private val typesByClass: MutableMap<KClass<*>, ObjectType<Any>> = HashMap()
        private val typesByEntityName: MutableMap<String, ObjectType<Any>> = HashMap()

        /**
         * Resolve KClass to ObjectType
         *
         * @throws UnknownObjectTypeException
         */
        fun resolveType(klass: KClass<*>): ObjectType<Any> {
            return recResolveType(klass)
                    ?: throw UnknownObjectTypeException("Unknown ObjectType requested to resolve by klass -- ${klass.qualifiedName}")
        }

        private fun recResolveType(klass: KClass<*>): ObjectType<Any>? {
            return typesByClass[klass] ?: klass.superclasses
                    .map { recResolveType(it) }
                    .firstOrNull { it != null }
        }

        /**
         * Resolve stateName to ObjectType
         *
         * @throws UnknownObjectTypeException
         */
        fun resolveType(name: String): ObjectType<Any> {
            return typesByEntityName[name]
                    ?: throw UnknownObjectTypeException("Unknown ObjectType requested to resolve by name -- $name")
        }

        @Synchronized
        internal fun addType(type: ObjectType<Any>) {
            typesByClass[type.klass] = type
            typesByEntityName[type.entityName] = type
        }

        internal fun getTypes(): Set<ObjectType<Any>> = typesByClass.values.toSet()

        @TestOnly
        @Synchronized
        internal fun clearTypes() {
            typesByClass.clear()
            typesByEntityName.clear()
        }
    }
}
