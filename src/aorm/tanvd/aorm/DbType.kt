package tanvd.aorm

import org.joda.time.DateTime
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.Timestamp
import java.util.*

sealed class DbType<T> {
    abstract fun toSqlName() : String

    abstract fun getValue(name: String, result: ResultSet) : T
    abstract fun setValue(index: Int, statement: PreparedStatement, value: T)
}

sealed class DbPrimitiveType<T>: DbType<T>() {
    abstract fun toArray(): DbArrayType<T>
}

sealed class DbArrayType<T> : DbType<List<T>>() {
    abstract fun toPrimitive(): DbPrimitiveType<T>
}

class DbDate : DbPrimitiveType<Date>() {
    override fun toSqlName(): String {
        return "Date"
    }

    override fun getValue(name: String, result: ResultSet): Date {
        return result.getDate(name)
    }

    override fun setValue(index: Int, statement: PreparedStatement, value: Date) {
        statement.setDate(index, java.sql.Date(value.time))
    }

    override fun toArray(): DbArrayType<Date> {
        return DbArrayDate()
    }
}

class DbDateTime: DbPrimitiveType<DateTime>() {
    override fun toSqlName(): String {
        return "DateTime"
    }

    override fun getValue(name: String, result: ResultSet): DateTime {
        return DateTime(result.getTimestamp(name).time)
    }

    override fun setValue(index: Int, statement: PreparedStatement, value: DateTime) {
        statement.setTimestamp(index, Timestamp(value.millis))
    }

    override fun toArray(): DbArrayType<DateTime> {
        return DbArrayDateTime()
    }
}

class DbLong: DbPrimitiveType<Long>() {
    override fun toSqlName(): String {
        return "Int64"
    }

    override fun getValue(name: String, result: ResultSet): Long {
        return result.getLong(name)
    }

    override fun setValue(index: Int, statement: PreparedStatement, value: Long) {
        statement.setLong(index, value)
    }

    override fun toArray(): DbArrayType<Long> {
        return DbArrayLong()
    }
}

class DbULong: DbPrimitiveType<Long>() {
    override fun toSqlName(): String {
        return "UInt64"
    }

    override fun getValue(name: String, result: ResultSet): Long {
        return result.getLong(name)
    }

    override fun setValue(index: Int, statement: PreparedStatement, value: Long) {
        statement.setLong(index, value)
    }

    override fun toArray(): DbArrayType<Long> {
        return DbArrayULong()
    }
}

class DbBoolean: DbPrimitiveType<Boolean>() {
    override fun toSqlName(): String {
        return "UInt8"
    }

    override fun getValue(name: String, result: ResultSet): Boolean {
        return result.getInt(name) == 1
    }

    override fun setValue(index: Int, statement: PreparedStatement, value: Boolean) {
        if (value) {
            statement.setInt(index, 1)
        } else {
            statement.setInt(index, 0)
        }
    }

    override fun toArray(): DbArrayType<Boolean> {
        return DbArrayBoolean()
    }
}

class DbString: DbPrimitiveType<String>() {
    override fun toSqlName(): String {
        return "String"
    }

    override fun getValue(name: String, result: ResultSet): String {
        return result.getString(name)
    }

    override fun setValue(index: Int, statement: PreparedStatement, value: String) {
        statement.setString(index, value)
    }

    override fun toArray(): DbArrayType<String> {
        return DbArrayString()
    }
}

class DbArrayDate: DbArrayType<Date>() {
    override fun toSqlName(): String {
        return "Array(Date)"
    }

    override fun getValue(name: String, result: ResultSet): List<Date> {
        return (result.getArray(name).array as Array<Date>).toList()
    }

    override fun setValue(index: Int, statement: PreparedStatement, value: List<Date>) {
        statement.setArray(index,
                statement.connection.createArrayOf(toSqlName(), value.map {
                    java.sql.Date(it.time)
                }.toTypedArray()))
    }

    override fun toPrimitive(): DbPrimitiveType<Date> {
        return DbDate()
    }
}

class DbArrayDateTime: DbArrayType<DateTime>() {
    override fun toSqlName(): String {
        return "Array(DateTime)"
    }

    override fun getValue(name: String, result: ResultSet): List<DateTime> {
        return (result.getArray(name).array as Array<Timestamp>).map {
            DateTime(result.getTimestamp(name).nanos / 1000)
        }
    }

    override fun setValue(index: Int, statement: PreparedStatement, value: List<DateTime>) {
        statement.setArray(index,
                statement.connection.createArrayOf(toSqlName(), value.map {
                    Timestamp(it.millis)
                }.toTypedArray()))
    }

    override fun toPrimitive(): DbPrimitiveType<DateTime> {
        return DbDateTime()
    }

}

class DbArrayLong: DbArrayType<Long>() {
    override fun toSqlName(): String {
        return "Array(Int64)"
    }

    override fun getValue(name: String, result: ResultSet): List<Long> {
        return (result.getArray(name).array as LongArray).toList()
    }

    override fun setValue(index: Int, statement: PreparedStatement, value: List<Long>) {
        statement.setArray(index,
                statement.connection.createArrayOf(toSqlName(), value.toTypedArray()))
    }

    override fun toPrimitive(): DbPrimitiveType<Long> {
        return DbLong()
    }
}

class DbArrayULong: DbArrayType<Long>() {
    override fun toSqlName(): String {
        return "Array(UInt64)"
    }

    override fun getValue(name: String, result: ResultSet): List<Long> {
        return (result.getArray(name).array as LongArray).toList()
    }

    override fun setValue(index: Int, statement: PreparedStatement, value: List<Long>) {
        statement.setArray(index,
                statement.connection.createArrayOf(toSqlName(), value.toTypedArray()))
    }

    override fun toPrimitive(): DbPrimitiveType<Long> {
        return DbULong()
    }
}

class DbArrayBoolean: DbArrayType<Boolean>() {
    override fun toSqlName(): String {
        return "Array(UInt8)"
    }

    override fun getValue(name: String, result: ResultSet): List<Boolean> {
        return (result.getArray(name).array as LongArray).toList().map {
            it.toInt() == 1
        }.toList()
    }

    override fun setValue(index: Int, statement: PreparedStatement, value: List<Boolean>) {
        statement.setArray(index,
                statement.connection.createArrayOf(toSqlName(), value.map { if (it) 1 else 0 }.toTypedArray()))
    }

    override fun toPrimitive(): DbPrimitiveType<Boolean> {
        return DbBoolean()
    }
}

class DbArrayString: DbArrayType<String>() {
    override fun toSqlName(): String {
        return "Array(String)"
    }

    override fun getValue(name: String, result: ResultSet): List<String> {
        return (result.getArray(name).array as Array<String>).toList()
    }

    override fun setValue(index: Int, statement: PreparedStatement, value: List<String>) {
        statement.setArray(index,
                statement.connection.createArrayOf(toSqlName(), value.toTypedArray()))
    }

    override fun toPrimitive(): DbPrimitiveType<String> {
        return DbString()
    }
}
