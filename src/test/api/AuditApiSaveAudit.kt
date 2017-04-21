package api

import org.mockito.Mockito.reset
import org.mockito.Mockito.verify
import org.powermock.api.mockito.PowerMockito.`when`
import org.powermock.api.mockito.PowerMockito.mock
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
import tanvd.audit.model.external.AuditSerializer
import tanvd.audit.model.external.AuditType
import tanvd.audit.model.internal.AuditRecordInternal
import utils.TypeUtils
import java.util.concurrent.BlockingQueue


@PowerMockIgnore("javax.management.*", "javax.xml.parsers.*", "com.sun.org.apache.xerces.internal.jaxp.*", "ch.qos.logback.*", "org.slf4j.*")
@PrepareForTest(AuditExecutor::class, AuditType::class)
internal class AuditApiSaveAudit : PowerMockTestCase() {

    class TestClassFirst {
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
        auditDao = mock(AuditDao::class.java)
        auditExecutor = mock(AuditExecutor::class.java)
        @Suppress("UNCHECKED_CAST")
        auditQueueInternal = mock(BlockingQueue::class.java) as BlockingQueue<AuditRecordInternal>
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
        reset(auditDao)
        reset(auditExecutor)
        reset(auditQueueInternal)
        TypeUtils.clearTypes()
    }

    @Test
    fun saveObjects_objectsSaved_AppropriateAuditRecordAddedToQueue() {
        addPrimitiveTypesAndTestClassFirst()
        val auditRecord = fullAuditRecord()
        isQueueFullOnRecord(auditRecord, false)

        auditApi!!.saveAudit("123", 456, TestClassFirst(), unixTimeStamp = 789L)

        Assert.assertEquals(auditRecordsNotCommitted?.get()?.size, 1)
        Assert.assertEquals(auditRecordsNotCommitted?.get(), listOf(auditRecord))
    }

    @Test
    fun saveObjectsWithExceptions_objectsSaved_AppropriateAuditRecordAddedToQueue() {
        addPrimitiveTypesAndTestClassFirst()
        val auditRecord = fullAuditRecord()
        isQueueFullOnRecord(auditRecord, false)

        auditApi!!.saveAuditWithExceptions("123", 456, TestClassFirst(), unixTimeStamp = 789L)

        Assert.assertEquals(auditRecordsNotCommitted?.get()?.size, 1)
        Assert.assertEquals(auditRecordsNotCommitted?.get(), listOf(auditRecord))
    }

    @Test
    fun saveObjects_unknownType_ObjectIgnored() {
        auditApi!!.addPrimitiveTypes()
        val auditRecord = auditRecordWithoutTestClassFirst()
        isQueueFullOnRecord(auditRecord, false)

        auditApi!!.saveAudit("123", 456, TestClassFirst(), unixTimeStamp = 789L)

        Assert.assertEquals(auditRecordsNotCommitted?.get()?.size, 1)
        Assert.assertEquals(auditRecordsNotCommitted?.get(), listOf(auditRecord))
    }

    @Test
    fun saveObjectsWithExceptions_unknownType_ExceptionThrown() {
        auditApi!!.addPrimitiveTypes()

        try {
            auditApi?.saveAuditWithExceptions("123", 456, TestClassFirst(), unixTimeStamp = 789L)
        } catch (e: UnknownAuditTypeException) {
            return
        }
        Assert.fail()
    }

    private fun fullAuditRecord(): AuditRecordInternal {
        return AuditRecordInternal(listOf(
                Pair(AuditType.resolveType(String::class), "123"),
                Pair(AuditType.resolveType(Int::class), "456"),
                Pair(AuditType.resolveType(TestClassFirst::class), "TestClassFirstId")
        ), 789L)
    }

    private fun auditRecordWithoutTestClassFirst(): AuditRecordInternal {
        return AuditRecordInternal(listOf(
                Pair(AuditType.resolveType(String::class), "123"),
                Pair(AuditType.resolveType(Int::class), "456")
        ), 789L)
    }

    private fun addPrimitiveTypesAndTestClassFirst() {
        auditApi!!.addPrimitiveTypes()
        val type = AuditType(TestClassFirst::class, "TestClassFirst", TestClassFirst)
        auditApi!!.addTypeForAudit(type)
    }

    private fun isQueueFullOnRecord(record: AuditRecordInternal, full: Boolean) {
        `when`(auditQueueInternal!!.offer(record)).thenReturn(!full)
    }
}
