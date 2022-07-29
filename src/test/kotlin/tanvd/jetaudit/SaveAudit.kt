package tanvd.jetaudit

import org.junit.*
import org.junit.runner.RunWith
import org.mockito.Mockito.reset
import org.powermock.api.mockito.PowerMockito.`when`
import org.powermock.api.mockito.PowerMockito.mock
import org.powermock.core.classloader.annotations.PowerMockIgnore
import org.powermock.core.classloader.annotations.PrepareForTest
import org.powermock.modules.junit4.PowerMockRunner
import tanvd.jetaudit.implementation.*
import tanvd.jetaudit.implementation.clickhouse.AuditDao
import tanvd.jetaudit.model.external.records.InformationObject
import tanvd.jetaudit.model.external.types.objects.ObjectType
import tanvd.jetaudit.model.internal.AuditRecordInternal
import tanvd.jetaudit.utils.*
import tanvd.jetaudit.utils.SamplesGenerator.getRecordInternal
import java.util.*
import java.util.concurrent.BlockingQueue


@RunWith(PowerMockRunner::class)
@PowerMockIgnore("javax.management.*", "javax.xml.parsers.*", "javax.net.*",
        "jdk.*",
        "com.sun.org.apache.xerces.internal.jaxp.*", "ch.qos.logback.*",
        "org.slf4j.*", "kotlin.*")
@PrepareForTest(AuditExecutor::class, ObjectType::class)
internal class SaveAudit {
    private var currentId = 0L

    private var auditDao: AuditDao? = null

    private var auditExecutor: AuditExecutor? = null

    private var auditQueueInternal: BlockingQueue<QueueCommand>? = null

    private var auditRecordsNotCommitted: ThreadLocal<ArrayList<AuditRecordInternal>>? = null

    private var auditApi: AuditAPI? = null

    @Before
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
        auditApi = AuditAPI(auditDao!!, auditExecutor!!, auditQueueInternal!!, auditRecordsNotCommitted!!, DbUtils.getProperties(), DbUtils.getDataSource())
    }

    @After
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
    fun saveObjects_unknownType_ObjectIgnored() {
        addPrimitiveTypes()

        val information = getSampleInformation()
        val auditRecord = auditRecordWithoutTestClassFirst(information)
        isQueueFullOnRecord(auditRecord, false)

        auditApi!!.save("123", 456, TestClassString("string"), information = information)

        Assert.assertEquals(auditRecordsNotCommitted?.get(), listOf(auditRecord))
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
