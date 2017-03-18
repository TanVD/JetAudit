package Clickhouse.benchmark

import org.testng.annotations.BeforeClass
import org.testng.annotations.Test
import tanvd.audit.implementation.clickhouse.JdbcClickhouseConnection
import tanvd.audit.implementation.clickhouse.model.*
import java.math.BigInteger
import java.security.SecureRandom
import java.sql.DriverManager
import java.util.*


internal class ClickhouseBenchmark() {

    companion object {
        val random = SecureRandom()
        var connection : JdbcClickhouseConnection? = null
    }

    @Suppress("UNCHECKED_CAST")
    @BeforeClass
    fun createAll() {
        val rawConnection = DriverManager.getConnection("jdbc:clickhouse://localhost:8123/benchmark", "default", "")
        connection = JdbcClickhouseConnection(rawConnection)
        connection!!.createTable("STRINGS", DbTableHeader(arrayListOf(
                DbColumnHeader("date_time", DbColumnType.DbDate),
                DbColumnHeader("arrays", DbColumnType.DbArrayString))),
                "date_time", "date_time")
    }

    @Test
    fun saveRows() {
        for (counter in 1..40) {
            val stringsAll = ArrayList<DbRow>()
            for (i in 1..30000) {
                val numberOfStrings = random.nextInt(10)
                val arrayOfString = ArrayList<String>()
                for (j in 1..numberOfStrings) {
                    arrayOfString.add(BigInteger(130, random).toString(32))
                }
                stringsAll.add(DbRow(arrayListOf(DbColumn("arrays", arrayOfString, DbColumnType.DbArrayString))))
            }

            connection!!.insertRows("STRINGS", DbTableHeader(arrayListOf(
                    DbColumnHeader("arrays", DbColumnType.DbArrayString))),
                    stringsAll)
            println("Saved 30000")
        }
    }

    @Test
    fun loadRows() {
        connection!!.loadRows("STRINGS", "arrays", "1234")
    }

}
