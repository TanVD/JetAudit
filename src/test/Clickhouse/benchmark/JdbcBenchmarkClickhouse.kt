package Clickhouse.benchmark

import org.testng.annotations.BeforeClass
import ru.yandex.clickhouse.ClickHouseDataSource
import ru.yandex.clickhouse.settings.ClickHouseProperties
import tanvd.audit.implementation.clickhouse.JdbcClickhouseConnection
import tanvd.audit.implementation.clickhouse.model.*
import tanvd.audit.model.QueryParameters
import java.math.BigInteger
import java.security.SecureRandom
import java.util.*


internal class JdbcBenchmarkClickhouse() {

    companion object {
        val random = SecureRandom()
        var connection: JdbcClickhouseConnection? = null
    }

    @Suppress("UNCHECKED_CAST")
    @BeforeClass
    fun createAll() {
        val properties = ClickHouseProperties()
        properties.user = "default"
        properties.password = ""
        val dataSource = ClickHouseDataSource("jdbc:clickhouse://localhost:8123/benchmark", properties)
        connection = JdbcClickhouseConnection(dataSource)
        connection!!.createTable("STRINGS", DbTableHeader(arrayListOf(
                DbColumnHeader("date_time", DbColumnType.DbDate),
                DbColumnHeader("arrays", DbColumnType.DbArrayString))),
                "date_time", "date_time")
    }

    //@Test
    fun checkForInjection() {
        connection!!.insertRow("STRINGS",
                DbRow(listOf(DbColumn("arrays", listOf("]); select * from benchmark.STRINGS;"), DbColumnType.DbArrayString))));
    }


    //@Test
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

            val time = System.currentTimeMillis()
            connection!!.insertRows("STRINGS", DbTableHeader(arrayListOf(
                    DbColumnHeader("arrays", DbColumnType.DbArrayString))),
                    stringsAll)
            println("Time: " + (System.currentTimeMillis() - time))
        }
    }

    //@Test
    fun loadRows() {
        val time = System.currentTimeMillis()
        connection!!.loadRows("STRINGS", "arrays", "''''\n\t\b''", DbTableHeader(listOf(
                DbColumnHeader("date_time", DbColumnType.DbDate),
                DbColumnHeader("arrays", DbColumnType.DbArrayString))), QueryParameters())
        println("Time: " + (System.currentTimeMillis() - time))
    }

}
