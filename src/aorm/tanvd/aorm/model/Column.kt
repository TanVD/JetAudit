package tanvd.aorm.model

import org.joda.time.DateTime
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.Timestamp
import java.util.*

abstract class Column<T : Any>(val name: String, val type: DbType) {
    var defaultFunction: (() -> T)? = null

    abstract fun getValue(result: ResultSet) : T
    abstract fun setValue(index: Int, statement: PreparedStatement, value: T)

    fun toSqlDef(): String {
        return "$name ${type.toSqlName()}"
    }

    /**
     * Equals by name.
     */
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as Column<*>

        if (name != other.name) return false

        return true
    }

    /**
     * Hashcode by name.
     */
    override fun hashCode(): Int {
        return name.hashCode()
    }
}

class DateColumn(name: String) : Column<Date>(name, DbDate()) {
    override fun getValue(result: ResultSet): Date {
        return result.getDate(name)
    }

    override fun setValue(index: Int, statement: PreparedStatement, value: Date) {
        statement.setDate(index, java.sql.Date(value.time))
    }
}

class DateTimeColumn(name: String) : Column<DateTime>(name, DbDateTime()) {
    override fun getValue(result: ResultSet): DateTime {
        return DateTime(result.getTimestamp(name).nanos / 1000)
    }

    override fun setValue(index: Int, statement: PreparedStatement, value: DateTime) {
        statement.setTimestamp(index, Timestamp(value.millis))
    }

}

class LongColumn(name: String) : Column<Long>(name, DbLong()) {
    override fun getValue(result: ResultSet): Long {
        return result.getLong(name)
    }

    override fun setValue(index: Int, statement: PreparedStatement, value: Long) {
        statement.setLong(index, value)
    }
}

class ULongColumn(name: String) : Column<Long>(name, DbULong()) {
    override fun getValue(result: ResultSet): Long {
        return result.getLong(name)
    }

    override fun setValue(index: Int, statement: PreparedStatement, value: Long) {
        statement.setLong(index, value)
    }
}

class BooleanColumn(name: String) : Column<Boolean>(name, DbBoolean()) {
    override fun getValue(result: ResultSet): Boolean {
        return result.getInt(name) == 1
    }

    override fun setValue(index: Int, statement: PreparedStatement, value: Boolean) {
        if (value) {
            statement.setInt(index, 1)
        } else {
            statement.setInt(index, 0)
        }
    }
}

class StringColumn(name: String) : Column<String>(name, DbString()) {
    override fun getValue(result: ResultSet): String {
        return result.getString(name)
    }

    override fun setValue(index: Int, statement: PreparedStatement, value: String) {
        statement.setString(index, value)
    }
}


class ArrayDateColumn(name: String) : Column<List<Date>>(name, DbArrayDate()) {
    override fun getValue(result: ResultSet): List<Date> {
        return (result.getArray(name).array as Array<Date>).toList()
    }

    override fun setValue(index: Int, statement: PreparedStatement, value: List<Date>) {
        statement.setArray(index,
                statement.connection.createArrayOf(type.toSqlName(), value.map {
                    java.sql.Date(it.time)
                }.toTypedArray()))
    }
}

class ArrayDateTimeColumn(name: String) : Column<List<DateTime>>(name, DbArrayDateTime()) {
    override fun getValue(result: ResultSet): List<DateTime> {
        return (result.getArray(name).array as Array<Timestamp>).map {
            DateTime(result.getTimestamp(name).nanos / 1000)
        }
    }

    override fun setValue(index: Int, statement: PreparedStatement, value: List<DateTime>) {
        statement.setArray(index,
                statement.connection.createArrayOf(type.toSqlName(), value.map {
                    Timestamp(it.millis)
                }.toTypedArray()))
    }

}

class ArrayLongColumn(name: String) : Column<List<Long>>(name, DbArrayLong()) {
    override fun getValue(result: ResultSet): List<Long> {
        return (result.getArray(name).array as Array<Long>).toList()
    }

    override fun setValue(index: Int, statement: PreparedStatement, value: List<Long>) {
        statement.setArray(index,
                statement.connection.createArrayOf(type.toSqlName(), value.toTypedArray()))
    }
}

class ArrayULongColumn(name: String) : Column<List<Long>>(name, DbArrayULong()) {
    override fun getValue(result: ResultSet): List<Long> {
        return (result.getArray(name).array as Array<Long>).toList()
    }

    override fun setValue(index: Int, statement: PreparedStatement, value: List<Long>) {
        statement.setArray(index,
                statement.connection.createArrayOf(type.toSqlName(), value.toTypedArray()))
    }
}

class ArrayBooleanColumn(name: String) : Column<List<Boolean>>(name, DbArrayBoolean()) {
    override fun getValue(result: ResultSet): List<Boolean> {
        return (result.getArray(name).array as Array<Int>).map {
            it == 1
        }.toList()
    }

    override fun setValue(index: Int, statement: PreparedStatement, value: List<Boolean>) {
        statement.setArray(index,
                statement.connection.createArrayOf(type.toSqlName(), value.map { if (it) 1 else 0 }.toTypedArray()))
    }
}

class ArrayStringColumn(name: String) : Column<List<String>>(name, DbArrayString()) {
    override fun getValue(result: ResultSet): List<String> {
        return (result.getArray(name).array as Array<String>).toList()
    }

    override fun setValue(index: Int, statement: PreparedStatement, value: List<String>) {
        statement.setArray(index,
                statement.connection.createArrayOf(type.toSqlName(), value.toTypedArray()))
    }
}






//Helper function
infix fun <T: Any>Column<T>.default(func: () -> T) : Column<T> {
    defaultFunction = func
    return this
}

