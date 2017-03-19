package Clickhouse.benchmark

import tanvd.audit.AuditAPI
import java.math.BigInteger
import java.security.SecureRandom
import java.util.*


internal class AuditApiBenchmarkClickhouse() {

    companion object {
        val random = SecureRandom()
        val auditApi: AuditAPI = AuditAPI("jdbc:clickhouse://localhost:8123/benchmark", "default", "")
    }

    //@Test
    fun saveRows() {
        val times = 1
        for (k in 1..times) {
            val records = ArrayList<ArrayList<Any>>()
            for (i in 1..20000) {
                val record = ArrayList<Any>()
                for (j in 1..30) {
                    val choice = random.nextInt(3)
                    when (choice) {
                        0 -> {
                            record.add(BigInteger(130, random).toString(32))
                        }
                        1 -> {
                            record.add(random.nextInt(500))
                        }
                        2 -> {
                            record.add(random.nextLong() )
                        }
                    }
                }
                records.add(record)
            }
            val time = System.currentTimeMillis()
            for (record in records) {
                auditApi.saveAudit(*(record.toArray()))
            }
            while (auditApi.executor.isStillWorking()) {
                Thread.sleep(20)
            }
            println("Time: " + (System.currentTimeMillis() - time))
        }
    }

    //@Test
    fun loadRows() {
        val times = 1
        for (i in 1..times) {
            val time = System.currentTimeMillis()
            val result = auditApi.loadAudit(Int::class, "477")
            println("Time: " + (System.currentTimeMillis() - time))
            println("Size: " + result.size)
        }
    }

}