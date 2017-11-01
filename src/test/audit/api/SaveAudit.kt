package audit.api

import org.mockito.Mockito.reset
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
import tanvd.audit.exceptions.UnknownObjectTypeException
import tanvd.audit.implementation.AuditExecutor
import tanvd.audit.implementation.QueueCommand
import tanvd.audit.implementation.SaveRecords
import tanvd.audit.implementation.dao.AuditDao
import tanvd.audit.model.external.records.InformationObject
import tanvd.audit.model.external.types.objects.ObjectType
import tanvd.audit.model.internal.AuditRecordInternal
import audit.utils.*
import audit.utils.SamplesGenerator.getRecordInternal
import java.util.*
import java.util.concurrent.BlockingQueue


@PowerMockIgnore("javax.management.*", "javax.xml.parsers.*", "com.sun.org.apache.xerces.internal.jaxp.*", "ch.qos.logback.*", "org.slf4j.*")
@PrepareForTest(AuditExecutor::class, ObjectType::class)
internal class SaveAudit : PowerMockTestCase() {

    private var currentId = 0L

    private var auditDao: AuditDao? = null

    private var auditExecutor: AuditExecutor? = null

    private var auditQueueInternal: BlockingQueue<QueueCommand>? = null

    private var auditRecordsNotCommitted: ThreadLocal<ArrayList<AuditRecordInternal>>? = null

    private var auditApi: AuditAPI? = null

    @BeforeClass
    fun setMocks() {
        auditDao = mock(AuditDao::class.java)
        auditExecutor = mock(AuditExecutor::class.java)
        @Suppress("UNCHECKED_CAST")
        auditQueueInternal = mock(BlockingQueue::class.java) as BlockingQueue<QueueCommand>
        auditRecordsNotCommitted = object : ThreadLocal<ArrayList<AuditRecordInternal>>() {
            override fun initialValue(): ArrayList<AuditRecordInternal>? {
                return ArrayList()
            }
        }
        auditApi = AuditAPI(auditDao!!, auditExecutor!!, auditQueueInternal!!, auditRecordsNotCommitted!!, DbUtils.getProperties())
    }

    @AfterMethod
    fun resetMocks() {
        currentId = 0
        auditRecordsNotCommitted!!.remove()
        reset(auditDao)
        reset(auditExecutor)
        reset(auditQueueInternal)
        TestUtil.clearTypes()
    }

    @Test
    fun saveObjects_objectsSaved_AppropriateAuditRecordAddedToQueue() {
        addPrimitiveTypesAndTestClassFirst()

        val information = getSampleInformation()
        val auditRecord = fullAuditRecord(information)
        isQueueFullOnRecord(auditRecord, false)

        auditApi!!.save("123", 456, TestClassString("string"), information = information)

        Assert.assertEquals(auditRecordsNotCommitted?.get(), listOf(auditRecord))
    }

    @Test
    fun saveObjectsWithExceptions_objectsSaved_AppropriateAuditRecordAddedToQueue() {
        addPrimitiveTypesAndTestClassFirst()

        val information = getSampleInformation()
        val auditRecord = fullAuditRecord(information)
        isQueueFullOnRecord(auditRecord, false)

        auditApi!!.saveWithException("123", 456, TestClassString("string"), information = information)

        Assert.assertEquals(auditRecordsNotCommitted?.get(), listOf(auditRecord))
    }

    @Test
    fun saveObjects_unknownType_ObjectIgnored() {
        addPrimitiveTypes()

        val information = getSampleInformation()
        val auditRecord = auditRecordWithoutTestClassFirst(information)
        isQueueFullOnRecord(auditRecord, false)

        auditApi!!.save("123", 456, TestClassString("string"), information = information)

        Assert.assertEquals(auditRecordsNotCommitted?.get(), listOf(auditRecord))
    }

    @Test
    fun saveObjectsWithExceptions_unknownType_ExceptionThrown() {
        addPrimitiveTypes()

        try {
            auditApi?.saveWithException("123", 456, TestClassString("string"), information = HashSet())
        } catch (e: UnknownObjectTypeException) {
            return
        }
        Assert.fail()
    }

    private fun fullAuditRecord(information: LinkedHashSet<InformationObject<*>>): AuditRecordInternal {
        return getRecordInternal("123", 456, TestClassString("string"), information = information)
    }

    private fun auditRecordWithoutTestClassFirst(information: LinkedHashSet<InformationObject<*>>): AuditRecordInternal {
        return getRecordInternal("123", 456, information = information)
    }

    private fun addPrimitiveTypes() {
        auditApi!!.addPrimitiveTypes()
    }

    private fun addPrimitiveTypesAndTestClassFirst() {
        auditApi!!.addPrimitiveTypes()
        val type = ObjectType(TestClassString::class, TestClassStringPresenter)
        auditApi!!.addObjectType(type)
    }

    private fun isQueueFullOnRecord(record: AuditRecordInternal, full: Boolean) {
        `when`(auditQueueInternal!!.offer(SaveRecords(record))).thenReturn(!full)
    }

    private fun getSampleInformation(): LinkedHashSet<InformationObject<*>> {
        return InformationUtils.getPrimitiveInformation(currentId++, 1, 2, SamplesGenerator.getMillenniumStart())
    }
}
