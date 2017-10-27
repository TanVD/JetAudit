package tanvd.aorm.model

import kotlin.reflect.KProperty

abstract class Entity(private val row: Row) {
    @Suppress("UNCHECKED_CAST")
    operator fun <T : Any> Column<T>.getValue(o: Entity, desc: KProperty<*>): T {
        return row[this as Column<Any>] as T
    }
}