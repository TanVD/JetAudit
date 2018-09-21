package tanvd.jetaudit

import org.mockito.Mockito
import org.powermock.api.mockito.PowerMockito
import org.powermock.core.classloader.annotations.PowerMockIgnore
import org.powermock.core.classloader.annotations.PrepareForTest
import org.powermock.modules.testng.PowerMockTestCase
import org.testng.Assert
import org.testng.annotations.AfterMethod
import org.testng.annotations.BeforeMethod
import org.testng.annotations.Test
import tanvd.jetaudit.exceptions.AuditQueueFullException
import tanvd.jetaudit.implementation.AuditExecutor
import tanvd.jetaudit.implementation.QueueCommand
import tanvd.jetaudit.implementation.SaveRecords
import tanvd.jetaudit.implementation.clickhouse.AuditDao
import tanvd.jetaudit.model.external.records.InformationObject
import tanvd.jetaudit.model.external.types.objects.ObjectType
import tanvd.jetaudit.model.internal.AuditRecordInternal
import tanvd.jetaudit.utils.*
import tanvd.jetaudit.utils.SamplesGenerator.getRecordInternal
import java.util.*
import java.util.concurrent.BlockingQueue

@Suppress("UNCHECKED_CAST")
@PowerMockIgnore("javax.management.*", "javax.xml.parsers.*", "com.sun.org.apache.xerces.internal.jaxp.*", "ch.qos.logback.*", "org.slf4j.*")
@PrepareForTest(AuditExecutor::class, ObjectType::class)
internal class CommitAudit : PowerMockTestCase() {

    private var currentId: Long = 0

    private var auditDao: AuditDao? = null

    private var auditExecutor: AuditExecutor? = null

    private var auditQueueInternal: BlockingQueue<QueueCommand>? = null

    private var auditRecordsNotCommitted: ThreadLocal<ArrayList<AuditRecordInternal>>? = null

    private var auditApi: AuditAPI? = null

    @BeforeMethod
    fun setMocks() {
        auditDao = PowerMockito.mock(AuditDao::class.java)
        auditExecutor = PowerMockito.mock(AuditExecutor::class.java)
        @Suppress("UNCHECKED_CAST")
        auditQueueInternal = PowerMockito.mock(BlockingQueue::class.java) as BlockingQueue<QueueCommand>
        auditRecordsNotCommitted = object : ThreadLocal<ArrayList<AuditRecordInternal>>() {
            override fun initialValue(): ArrayList<AuditRecordInternal>? {
                return ArrayList()
            }
        }
        auditApi = AuditAPI(auditDao!!, auditExecutor!!, auditQueueInternal!!, auditRecordsNotCommitted!!, DbUtils.getProperties(), DbUtils.getDataSource())
        auditApi!!.addServiceInformation()
        auditApi!!.addPrimitiveTypes()
    }

    @AfterMethod
    fun resetMocks() {
        auditRecordsNotCommitted!!.remove()
        TestUtil.clearTypes()
    }

    @Test
    fun commitAudit_noExceptions_recordAddedToAuditQueue() {
        val testSet = arrayOf("123", 456, TestClassString("string"))
        val testStamp = 789L
        addTestClassFirst()
        val auditRecord = createAuditRecordInternal(*testSet, unixTimeStamp = testStamp)
        auditRecordsNotCommitted!!.get().add(auditRecord)

        auditApi!!.commit()

        Mockito.verify(auditQueueInternal)!!.add(SaveRecords(auditRecord))
    }

    @Test
    fun commitAuditWithExceptions_noExceptions_threadGroupDeleted() {
        val testSet = arrayOf("123", 456, TestClassString("string"))
        val testStamp = 789L
        addTestClassFirst()
        val auditRecord = createAuditRecordInternal(*testSet, unixTimeStamp = testStamp)
        auditRecordsNotCommitted!!.get().add(auditRecord)

        auditApi!!.commit()

        Assert.assertEquals(auditRecordsNotCommitted?.get()?.size, 0)
    }

    @Test
    fun commitAudit_queueFull_noExceptions() {
        val testSet = arrayOf("123", 456, TestClassString("string"))
        val testStamp = 789L
        addTestClassFirst()
        val auditRecord = createAuditRecordInternal(*testSet, unixTimeStamp = testStamp)
        PowerMockito.`when`(auditQueueInternal!!.size).thenReturn(AuditAPI.capacityOfQueue)

        auditRecordsNotCommitted!!.get().add(auditRecord)

        auditApi!!.commit()
    }

    @Test
    fun commitAudit_queueFull_auditQueueInternalNotChanged() {
        val testSet = arrayOf("123", 456, TestClassString("string"))
        val testStamp = 789L
        addTestClassFirst()
        val auditRecord = createAuditRecordInternal(*testSet, unixTimeStamp = testStamp)
        PowerMockito.`when`(auditQueueInternal!!.size).thenReturn(AuditAPI.capacityOfQueue)

        auditRecordsNotCommitted!!.get().add(auditRecord)

        auditApi!!.commit()

        Mockito.verify(auditQueueInternal)!!.size
        Mockito.verifyNoMoreInteractions(auditQueueInternal)
    }

    @Test
    fun commitAudit_queueFull_notCommittedGroupNotDeleted() {
        val testSet = arrayOf("123", 456, TestClassString("string"))
        val testStamp = 789L
        addTestClassFirst()
        val auditRecord = createAuditRecordInternal(*testSet, unixTimeStamp = testStamp)
        PowerMockito.`when`(auditQueueInternal!!.size).thenReturn(AuditAPI.capacityOfQueue)

        auditRecordsNotCommitted!!.get().add(auditRecord)

        try {
            auditApi!!.commit()
        } catch (e: AuditQueueFullException) {
        }

        Assert.assertEquals(auditRecordsNotCommitted?.get(), arrayListOf(auditRecord))
    }


    private fun createAuditRecordInternal(vararg objects: Any, unixTimeStamp: Long): AuditRecordInternal {
        return getRecordInternal(*objects, information = getSampleInformation(unixTimeStamp))
    }

    private fun addTestClassFirst() {
        val type = ObjectType(TestClassString::class, TestClassStringPresenter)
        auditDao!!.addTypeInDbModel(type)
        ObjectType.addType(type as ObjectType<Any>)
    }

    private fun getSampleInformation(timeStamp: Long): LinkedHashSet<InformationObject<*>> {
        return InformationUtils.getPrimitiveInformation(currentId++, timeStamp, 2, SamplesGenerator.getMillenniumStart())
    }
}
