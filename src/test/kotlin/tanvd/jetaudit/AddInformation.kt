package tanvd.jetaudit

import org.junit.Assert.assertTrue
import org.junit.*
import org.junit.runner.RunWith
import org.mockito.Mockito.verify
import org.powermock.api.mockito.PowerMockito.mock
import org.powermock.core.classloader.annotations.PowerMockIgnore
import org.powermock.core.classloader.annotations.PrepareForTest
import org.powermock.modules.junit4.PowerMockRunner
import tanvd.jetaudit.exceptions.AddExistingInformationTypeException
import tanvd.jetaudit.implementation.AuditExecutor
import tanvd.jetaudit.implementation.QueueCommand
import tanvd.jetaudit.implementation.clickhouse.AuditDao
import tanvd.jetaudit.model.external.types.information.InformationType
import tanvd.jetaudit.model.internal.AuditRecordInternal
import tanvd.jetaudit.utils.*
import java.util.concurrent.BlockingQueue


@RunWith(PowerMockRunner::class)
@PowerMockIgnore("javax.management.*", "javax.xml.parsers.*", "javax.net.ssl.*",
        "jdk.*",
        "com.sun.org.apache.xerces.internal.jaxp.*", "ch.qos.logback.*",
        "org.slf4j.*")
@PrepareForTest(AuditExecutor::class, InformationType::class)
internal class AddInformation {

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
        auditApi!!.addServiceInformation()
        auditApi!!.addPrimitiveTypes()
    }

    @After
    fun resetMocks() {
        auditRecordsNotCommitted!!.remove()
        TestUtil.clearTypes()
    }


    @Test
    fun addInformation_typeAdded_typeToInformationTypesAdded() {
        auditApi?.addInformationType(BooleanInf)

        assertTrue(InformationType.getTypes().contains(BooleanInf as InformationType<*>))
    }

    @Test
    fun addType_typeAdded_typeToAuditDaoAdded() {
        auditApi?.addInformationType(BooleanInf)

        verify(auditDao)?.addInformationInDbModel(BooleanInf)
    }

    @Test
    fun addType_typeExistingAdded_exceptionThrown() {
        auditApi?.addInformationType(BooleanInf)

        try {
            auditApi?.addInformationType(BooleanInf)
        } catch (e: AddExistingInformationTypeException) {
            return
        }
        Assert.fail()
    }
}
