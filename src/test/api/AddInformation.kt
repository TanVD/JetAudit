package api

import org.mockito.Mockito.reset
import org.mockito.Mockito.verify
import org.powermock.api.mockito.PowerMockito.mock
import org.powermock.core.classloader.annotations.PowerMockIgnore
import org.powermock.core.classloader.annotations.PrepareForTest
import org.powermock.modules.testng.PowerMockTestCase
import org.testng.Assert
import org.testng.Assert.assertEquals
import org.testng.annotations.AfterMethod
import org.testng.annotations.BeforeClass
import org.testng.annotations.Test
import tanvd.audit.AuditAPI
import tanvd.audit.exceptions.AddExistingInformationTypeException
import tanvd.audit.implementation.AuditExecutor
import tanvd.audit.implementation.dao.AuditDao
import tanvd.audit.model.external.types.InformationType
import tanvd.audit.model.internal.AuditRecordInternal
import utils.BooleanPresenter
import utils.TypeUtils
import java.util.concurrent.BlockingQueue


@PowerMockIgnore("javax.management.*", "javax.xml.parsers.*", "com.sun.org.apache.xerces.internal.jaxp.*", "ch.qos.logback.*", "org.slf4j.*")
@PrepareForTest(AuditExecutor::class, InformationType::class)
internal class AddInformation : PowerMockTestCase() {

    private var auditDao: AuditDao? = null

    private var auditExecutor: AuditExecutor? = null

    private var auditQueueInternal: BlockingQueue<AuditRecordInternal>? = null

    private var auditRecordsNotCommitted: ThreadLocal<ArrayList<AuditRecordInternal>>? = null

    private var auditApi: AuditAPI? = null

    @BeforeClass
    fun setMocks() {
        auditDao = mock(AuditDao::class.java)
        auditExecutor = mock(AuditExecutor::class.java)
        @Suppress("UNCHECKED_CAST")
        auditQueueInternal = mock(BlockingQueue::class.java) as BlockingQueue<AuditRecordInternal>
        auditRecordsNotCommitted = object : ThreadLocal<ArrayList<AuditRecordInternal>>() {
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
    fun addInformation_typeAdded_typeToInformationTypesAdded() {
        val type = createSampleInformationType()

        auditApi?.addInformationType(type)

        assertEquals(setOf(type), InformationType.getTypes())
    }

    @Test
    fun addType_typeAdded_typeToAuditDaoAdded() {
        val type = createSampleInformationType()

        auditApi?.addInformationType(type)

        verify(auditDao)?.addInformationInDbModel(type)
    }

    @Test
    fun addType_typeExistingAdded_exceptionThrown() {
        val type = createSampleInformationType()

        auditApi?.addInformationType(type)

        try {
            auditApi?.addInformationType(type)
        } catch (e: AddExistingInformationTypeException) {
            return
        }
        Assert.fail()
    }

    @Suppress("UNCHECKED_CAST")
    private fun createSampleInformationType(): InformationType<Any> {
        return InformationType(BooleanPresenter, "BooleanPresenter", InformationType.InformationInnerType.Boolean) as
                InformationType<Any>
    }
}