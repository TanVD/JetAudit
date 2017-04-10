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
import tanvd.audit.exceptions.AddExistingAuditTypeException
import tanvd.audit.implementation.AuditExecutor
import tanvd.audit.implementation.dao.AuditDao
import tanvd.audit.model.external.AuditSerializer
import tanvd.audit.model.external.AuditType
import tanvd.audit.model.internal.AuditRecordInternal
import java.util.concurrent.BlockingQueue


@PowerMockIgnore("javax.management.*", "javax.xml.parsers.*", "com.sun.org.apache.xerces.internal.jaxp.*", "ch.qos.logback.*", "org.slf4j.*")
@PrepareForTest(AuditExecutor::class, AuditType::class)
internal class AuditApiAddType : PowerMockTestCase() {

    class TestClassFirst {
        companion object serializer : AuditSerializer<TestClassFirst> {
            override fun display(value: TestClassFirst): String {
                return "TestClassFirstDisplay"
            }

            override fun deserialize(serializedString: String): TestClassFirst {
                throw UnsupportedOperationException("not implemented")
            }

            override fun serialize(value: TestClassFirst): String {
                return "TestClassFirstId"
            }

        }
    }


    private var auditDao: AuditDao? = null

    private var auditExecutor: AuditExecutor? = null

    private var auditQueueInternal: BlockingQueue<AuditRecordInternal>? = null

    private var auditApi: AuditAPI? = null

    @BeforeClass
    fun setMocks() {
        auditDao = mock(AuditDao::class.java)
        auditExecutor = mock(AuditExecutor::class.java)
        auditQueueInternal = mock(BlockingQueue::class.java) as BlockingQueue<AuditRecordInternal>
        auditApi = AuditAPI(auditDao!!, auditExecutor!!, auditQueueInternal!!)
    }

    @AfterMethod
    fun resetMocks() {
        reset(auditDao)
        reset(auditExecutor)
        reset(auditQueueInternal)
        AuditType.clearTypes()
    }


    @Test
    fun addType_typeAdded_typeToAuditTypesAdded() {
        val type = createTestClassFirstType()

        auditApi?.addTypeForAudit(type)

        assertEquals(setOf(type), AuditType.getTypes())
    }

    @Test
    fun addType_typeAdded_typeToAuditDaoAdded() {
        val type = createTestClassFirstType()

        auditApi?.addTypeForAudit(type)

        verify(auditDao)?.addTypeInDbModel(type)
    }

    @Test
    fun addType_typeExistingAdded_exceptionThrown() {
        val type = createTestClassFirstType()

        auditApi?.addTypeForAudit(type)

        try {
            auditApi?.addTypeForAudit(type)
        } catch (e: AddExistingAuditTypeException) {
            return
        }
        Assert.fail()
    }

    private fun createTestClassFirstType(): AuditType<TestClassFirst> {
        return AuditType(TestClassFirst::class, "TestClassFirst", TestClassFirst);
    }
}
