package tanvd.aorm.model

import tanvd.aorm.model.implementation.InsertClickhouse
import tanvd.aorm.model.implementation.TableClickhouse
import tanvd.aorm.model.query.Query

abstract class Table(val name: String, val entityConstructor: (Row) -> Entity) {
    abstract val engine: Engine

    internal val columns: MutableList<Column<Any>> = ArrayList()

    private fun <T: Any>registerColumn(column: Column<T>): Column<T> {
        columns.add(column as Column<Any>)
        return column
    }

    fun date(name: String) = registerColumn(DateColumn(name))

    fun datetime(name: String) = registerColumn(DateTimeColumn(name))

    fun ulong(name: String) = registerColumn(ULongColumn(name))

    fun long(name: String) = registerColumn(LongColumn(name))

    fun boolean(name: String) = registerColumn(BooleanColumn(name))

    fun string(name: String) = registerColumn(StringColumn(name))


    fun arrayDate(name: String) = registerColumn(ArrayDateColumn(name))

    fun arrayDateTime(name: String) = registerColumn(ArrayDateTimeColumn(name))

    fun arrayULong(name: String) = registerColumn(ArrayULongColumn(name))

    fun arrayLong(name: String) = registerColumn(ArrayLongColumn(name))

    fun arrayBoolean(name: String) = registerColumn(ArrayBooleanColumn(name))

    fun arrayString(name: String) = registerColumn(ArrayStringColumn(name))



    //Table functions
    fun create() {
        TableClickhouse.create(this)
    }

    fun drop() {
        TableClickhouse.drop(this)
    }

    fun addColumn(column: Column<*>) {
        if (!columns.contains(column)) {
            TableClickhouse.addColumn(this, column)
            columns.add(column as Column<Any>)
        }
    }

    fun dropColumn(column: Column<*>) {
        if (columns.contains(column)) {
            TableClickhouse.dropColumn(this, column)
            columns.remove(column)
        }
    }

    fun select(): Query {
        @Suppress("UNCHECKED_CAST")
        return Query(this, columns as List<Column<Any>>)
    }

    fun insert(expression: InsertExpression) {
        InsertClickhouse.insert(expression)
    }

    internal fun toResult(row: Row): Entity {
        return entityConstructor(row)
    }
}