package tanvd.jetaudit.performance

import org.junit.*
import tanvd.jetaudit.AuditAPI
import tanvd.jetaudit.model.external.equal
import tanvd.jetaudit.model.external.presenters.IntPresenter
import tanvd.jetaudit.model.external.presenters.LongPresenter
import tanvd.jetaudit.utils.*
import java.util.*

internal class ApiPerformanceTest {

    private var auditApi: AuditAPI? = null

    @Before
    fun addTypes() {
        TestUtil.drop()
        auditApi = AuditAPI(DbUtils.getProperties(), DbUtils.getDataSource())
    }

    @After
    fun clear() {
        TestUtil.drop()
    }


    @Test
    fun oneHundredInSecond() {
        val records = ArrayList<Array<Any>>()
        val fixedId = 1L
        val totalNumber = 100L
        for (i in 1..totalNumber) {
            records.add(getRandomRecord(fixedId))
        }
        val time = measureTime {
            for (record in records) {
                auditApi!!.save(*record)
            }
            auditApi!!.commit()
            waitUntilRightCount({
                auditApi!!.count(LongPresenter.value equal fixedId) == totalNumber
            }, 10, 10)
            while (auditApi!!.executor.stillWorking()) {
                Thread.sleep(10)
            }
        }
        println("Total time: $time")
        Assert.assertTrue(time < 1000)
    }

    @Test
    fun threeHundredInSecond() {
        val records = ArrayList<Array<Any>>()
        val fixedId = 1L
        val totalNumber = 300L
        for (i in 1..totalNumber) {
            records.add(getRandomRecord(fixedId))
        }
        val time = measureTime {
            for (record in records) {
                auditApi!!.save(*record)
            }
            auditApi!!.commit()
            waitUntilRightCount({
                auditApi!!.count(IntPresenter.value equal fixedId) == totalNumber
            }, 10, 10)
            while (auditApi!!.executor.stillWorking()) {
                Thread.sleep(10)
            }
        }
        println("Total time: $time")
        Assert.assertTrue(time < 1000)
    }

    fun getRandomRecord(fixedId: Long): Array<Any> {
        val list = ArrayList<Any>()
        val choice = SamplesGenerator.getRandomInt(2)
        val number = SamplesGenerator.getRandomInt(30)
        for (i in 1..number) {
            when (choice) {
                0 -> {
                    list.add(SamplesGenerator.getRandomInt())
                }
                1 -> {
                    list.add(SamplesGenerator.getRandomString())
                }
                else -> {
                    error("Generated unexpected choice")
                }
            }
        }
        list.add(fixedId)
        return list.toTypedArray()
    }
}

