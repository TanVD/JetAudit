package api

import org.mockito.Mockito.verify
import org.powermock.api.mockito.PowerMockito.mock
import org.powermock.core.classloader.annotations.PowerMockIgnore
import org.powermock.core.classloader.annotations.PrepareForTest
import org.powermock.modules.testng.PowerMockTestCase
import org.testng.Assert
import org.testng.Assert.assertTrue
import org.testng.annotations.AfterMethod
import org.testng.annotations.BeforeMethod
import org.testng.annotations.Test
import tanvd.audit.AuditAPI
import tanvd.audit.exceptions.AddExistingInformationTypeException
import tanvd.audit.implementation.AuditExecutor
import tanvd.audit.implementation.QueueCommand
import tanvd.audit.implementation.clickhouse.AuditDao
import tanvd.audit.model.external.types.information.InformationType
import tanvd.audit.model.internal.AuditRecordInternal
import utils.BooleanInf
import utils.DbUtils
import utils.TestUtil
import java.util.concurrent.BlockingQueue


@Suppress("UNCHECKED_CAST")
@PowerMockIgnore("javax.management.*", "javax.xml.parsers.*", "com.sun.org.apache.xerces.internal.jaxp.*", "ch.qos.logback.*", "org.slf4j.*")
@PrepareForTest(AuditExecutor::class, InformationType::class)
internal class AddInformation : PowerMockTestCase() {

    private var auditDao: AuditDao? = null

    private var auditExecutor: AuditExecutor? = null

    private var auditQueueInternal: BlockingQueue<QueueCommand>? = null

    private var auditRecordsNotCommitted: ThreadLocal<ArrayList<AuditRecordInternal>>? = null

    private var auditApi: AuditAPI? = null

    @BeforeMethod
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

    @AfterMethod
    fun resetMocks() {
        auditRecordsNotCommitted!!.remove()
        TestUtil.clearTypes()
    }


    @Test
    fun addInformation_typeAdded_typeToInformationTypesAdded() {
        auditApi?.addInformationType(BooleanInf)

        assertTrue(InformationType.getTypes().contains(BooleanInf as InformationType<Any>))
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