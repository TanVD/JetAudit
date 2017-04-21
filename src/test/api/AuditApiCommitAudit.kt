package api

import org.mockito.Mockito
import org.powermock.api.mockito.PowerMockito
import org.powermock.core.classloader.annotations.PowerMockIgnore
import org.powermock.core.classloader.annotations.PrepareForTest
import org.powermock.modules.testng.PowerMockTestCase
import org.testng.Assert
import org.testng.annotations.AfterMethod
import org.testng.annotations.BeforeClass
import org.testng.annotations.Test
import tanvd.audit.AuditAPI
import tanvd.audit.exceptions.AuditQueueFullException
import tanvd.audit.exceptions.UnknownAuditTypeException
import tanvd.audit.implementation.AuditExecutor
import tanvd.audit.implementation.dao.AuditDao
import tanvd.audit.model.external.*
import tanvd.audit.model.internal.AuditRecordInternal
import utils.TypeUtils
import java.util.concurrent.BlockingQueue

@PowerMockIgnore("javax.management.*", "javax.xml.parsers.*", "com.sun.org.apache.xerces.internal.jaxp.*", "ch.qos.logback.*", "org.slf4j.*")
@PrepareForTest(AuditExecutor::class, AuditType::class)
internal class AuditApiCommitAudit : PowerMockTestCase() {

    data class TestClassFirst(val hash: Int = 150) {

        companion object serializer : AuditSerializer<TestClassFirst> {
            override fun display(value: TestClassFirst): String {
                return "TestClassFirstDisplay"
            }

            override fun deserialize(serializedString: String): TestClassFirst {
                if (serializedString == "TestClassFirstId") {
                    return TestClassFirst()
                } else {
                    throw IllegalArgumentException()
                }
            }

            override fun serialize(value: TestClassFirst): String {
                return "TestClassFirstId"
            }

        }
    }


    private var auditDao: AuditDao? = null

    private var auditExecutor: AuditExecutor? = null

    private var auditQueueInternal: BlockingQueue<AuditRecordInternal>? = null

    private var auditRecordsNotCommitted:  ThreadLocal<ArrayList<AuditRecordInternal>>? = null

    private var auditApi: AuditAPI? = null

    @BeforeClass
    fun setMocks() {
        auditDao = PowerMockito.mock(AuditDao::class.java)
        auditExecutor = PowerMockito.mock(AuditExecutor::class.java)
        auditQueueInternal = PowerMockito.mock(BlockingQueue::class.java) as BlockingQueue<AuditRecordInternal>
        auditRecordsNotCommitted = object : ThreadLocal<ArrayList<AuditRecordInternal>>(){
            override fun initialValue(): ArrayList<AuditRecordInternal>? {
                return ArrayList()
            }
        }
        auditApi = AuditAPI(auditDao!!, auditExecutor!!, auditQueueInternal!!, auditRecordsNotCommitted!!)
    }

    @AfterMethod
    fun resetMocks() {
        auditRecordsNotCommitted!!.remove()
        Mockito.reset(auditDao)
        Mockito.reset(auditExecutor)
        Mockito.reset(auditQueueInternal)
        TypeUtils.clearTypes()
    }

    @Test
    fun commitAudit_noExceptions_recordAddedToAuditQueue() {
        val testSet = arrayOf("123", 456, TestClassFirst())
        val testStamp = 789L
        addPrimitiveTypesAndTestClassFirst()
        val auditRecord = createAuditRecordInternal(*testSet, unixTimeStamp = testStamp)
        auditRecordsNotCommitted!!.get().add(auditRecord)

        auditApi!!.commitAudit()

        Mockito.verify(auditQueueInternal)!!.addAll(listOf(auditRecord))
    }

    @Test
    fun commitAudit_noExceptions_threadGroupDeleted() {
        val testSet = arrayOf("123", 456, TestClassFirst())
        val testStamp = 789L
        addPrimitiveTypesAndTestClassFirst()
        val auditRecord = createAuditRecordInternal(*testSet, unixTimeStamp = testStamp)
        auditRecordsNotCommitted!!.get().add(auditRecord)

        auditApi!!.commitAuditWithExceptions()

        Assert.assertEquals(auditRecordsNotCommitted?.get()?.size, 0)
    }

    @Test
    fun commitAuditWithExceptions_noExceptions_recordAddedToAuditQueue() {
        val testSet = arrayOf("123", 456, TestClassFirst())
        val testStamp = 789L
        addPrimitiveTypesAndTestClassFirst()
        val auditRecord = createAuditRecordInternal(*testSet, unixTimeStamp = testStamp)
        auditRecordsNotCommitted!!.get().add(auditRecord)

        auditApi!!.commitAuditWithExceptions()

        Mockito.verify(auditQueueInternal)!!.addAll(arrayListOf(auditRecord))
    }

    @Test
    fun commitAuditWithExceptions_noExceptions_threadGroupDeleted() {
        val testSet = arrayOf("123", 456, TestClassFirst())
        val testStamp = 789L
        addPrimitiveTypesAndTestClassFirst()
        val auditRecord = createAuditRecordInternal(*testSet, unixTimeStamp = testStamp)
        auditRecordsNotCommitted!!.get().add(auditRecord)

        auditApi!!.commitAudit()

        Assert.assertEquals(auditRecordsNotCommitted?.get()?.size, 0)
    }

    @Test
    fun commitAudit_queueFull_noExceptions() {
        val testSet = arrayOf("123", 456, TestClassFirst())
        val testStamp = 789L
        addPrimitiveTypesAndTestClassFirst()
        val auditRecord = createAuditRecordInternal(*testSet, unixTimeStamp = testStamp)
        PowerMockito.`when`(auditQueueInternal!!.size).thenReturn(AuditAPI.capacityOfQueue)

        auditRecordsNotCommitted!!.get().add(auditRecord)

        auditApi!!.commitAudit()
    }

    @Test
    fun commitAudit_queueFull_auditQueueInternalNotChanged() {
        val testSet = arrayOf("123", 456, TestClassFirst())
        val testStamp = 789L
        addPrimitiveTypesAndTestClassFirst()
        val auditRecord = createAuditRecordInternal(*testSet, unixTimeStamp = testStamp)
        PowerMockito.`when`(auditQueueInternal!!.size).thenReturn(AuditAPI.capacityOfQueue)

        auditRecordsNotCommitted!!.get().add(auditRecord)

        auditApi!!.commitAudit()

        Mockito.verify(auditQueueInternal)!!.size
        Mockito.verifyNoMoreInteractions(auditQueueInternal)
    }

    @Test
    fun commitAudit_queueFull_notCommittedGroupNotDeleted() {
        val testSet = arrayOf("123", 456, TestClassFirst())
        val testStamp = 789L
        addPrimitiveTypesAndTestClassFirst()
        val auditRecord = createAuditRecordInternal(*testSet, unixTimeStamp = testStamp)
        PowerMockito.`when`(auditQueueInternal!!.size).thenReturn(AuditAPI.capacityOfQueue)

        auditRecordsNotCommitted!!.get().add(auditRecord)

        try {
            auditApi!!.commitAudit()
        } catch (e: AuditQueueFullException) {
        }

        Assert.assertEquals(auditRecordsNotCommitted?.get(), arrayListOf(auditRecord))
    }

    @Test
    fun commitAuditWithExceptions_queueFull_exceptionThrown() {
        val testSet = arrayOf("123", 456, TestClassFirst())
        val testStamp = 789L
        addPrimitiveTypesAndTestClassFirst()
        val auditRecord = createAuditRecordInternal(*testSet, unixTimeStamp = testStamp)
        PowerMockito.`when`(auditQueueInternal!!.size).thenReturn(AuditAPI.capacityOfQueue)

        auditRecordsNotCommitted!!.get().add(auditRecord)

        try {
            auditApi!!.commitAuditWithExceptions()
        } catch (e: AuditQueueFullException) {
            return
        }

        Assert.fail()
    }

    @Test
    fun commitAuditWithExceptions_queueFull_auditQueueInternalNotChanged() {
        val testSet = arrayOf("123", 456, TestClassFirst())
        val testStamp = 789L
        addPrimitiveTypesAndTestClassFirst()
        val auditRecord = createAuditRecordInternal(*testSet, unixTimeStamp = testStamp)
        PowerMockito.`when`(auditQueueInternal!!.size).thenReturn(AuditAPI.capacityOfQueue)

        auditRecordsNotCommitted!!.get().add(auditRecord)

        try {
            auditApi!!.commitAuditWithExceptions()
        } catch (e: AuditQueueFullException) {
        }

        Mockito.verify(auditQueueInternal)!!.size
        Mockito.verifyNoMoreInteractions(auditQueueInternal)
    }


    @Test
    fun commitAuditWithExceptions_queueFull_notCommittedGroupNotDeleted() {
        val testSet = arrayOf("123", 456, TestClassFirst())
        val testStamp = 789L
        addPrimitiveTypesAndTestClassFirst()
        val auditRecord = createAuditRecordInternal(*testSet, unixTimeStamp = testStamp)
        PowerMockito.`when`(auditQueueInternal!!.size).thenReturn(AuditAPI.capacityOfQueue)

        auditRecordsNotCommitted!!.get().add(auditRecord)

        try {
            auditApi!!.commitAuditWithExceptions()
        } catch (e: AuditQueueFullException) {
        }

        Assert.assertEquals(auditRecordsNotCommitted?.get(), arrayListOf(auditRecord))
    }




    private fun createAuditRecordInternal(vararg objects: Any, unixTimeStamp: Long): AuditRecordInternal {
        return AuditRecordInternal(*objects, unixTimeStamp = unixTimeStamp)
    }

    private fun addPrimitiveTypesAndTestClassFirst() {
        auditApi!!.addPrimitiveTypes()
        val type = AuditType(TestClassFirst::class, "TestClassFirst", TestClassFirst)
        auditApi!!.addTypeForAudit(type)
    }
}
