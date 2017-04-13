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
import tanvd.audit.exceptions.UnknownAuditTypeException
import tanvd.audit.implementation.AuditExecutor
import tanvd.audit.implementation.dao.AuditDao
import tanvd.audit.model.external.*
import tanvd.audit.model.internal.AuditRecordInternal
import java.util.concurrent.BlockingQueue

@PowerMockIgnore("javax.management.*", "javax.xml.parsers.*", "com.sun.org.apache.xerces.internal.jaxp.*", "ch.qos.logback.*", "org.slf4j.*")
@PrepareForTest(AuditExecutor::class, AuditType::class)
internal class AuditApiLoadAudit : PowerMockTestCase() {

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

    private var auditApi: AuditAPI? = null

    @BeforeClass
    fun setMocks() {
        auditDao = PowerMockito.mock(AuditDao::class.java)
        auditExecutor = PowerMockito.mock(AuditExecutor::class.java)
        auditQueueInternal = PowerMockito.mock(BlockingQueue::class.java) as BlockingQueue<AuditRecordInternal>
        auditApi = AuditAPI(auditDao!!, auditExecutor!!, auditQueueInternal!!)
    }

    @AfterMethod
    fun resetMocks() {
        Mockito.reset(auditDao)
        Mockito.reset(auditExecutor)
        Mockito.reset(auditQueueInternal)
        AuditType.clearTypes()
    }

    @Test
    fun loadAudit_recordLoaded_AppropriateAuditRecordReturned() {
        val testSet = arrayOf("123", 456, TestClassFirst())
        val testStamp = 789L
        addPrimitiveTypesAndTestClassFirst()
        val auditRecord = createAuditRecordInternal(*testSet, unixTimeStamp = testStamp)
        val expression = createExpressionString()
        val parameters = createSimpleParam()
        returnRecordOnExpressionAndParam(auditRecord, expression, parameters)

        val result = auditApi!!.loadAudit(expression, parameters)

        Assert.assertEquals(result, listOf(fullAuditRecord(*testSet, unixTimeStamp = testStamp)))
    }

    @Test
    fun loadAudit_recordsLoaded_AppropriateAuditRecordsReturned() {
        val testSetFirst = arrayOf("123", 456, TestClassFirst())
        val testStampFirst = 1L
        val testSetSecond = arrayOf("123", 789)
        val testStampSecond = 2L
        addPrimitiveTypesAndTestClassFirst()
        val auditRecords = listOf(
                createAuditRecordInternal(*testSetFirst, unixTimeStamp = testStampFirst),
                createAuditRecordInternal(*testSetSecond, unixTimeStamp = testStampSecond)
        )
        val expression = createExpressionString()
        val parameters = createSimpleParam()
        returnRecordsOnExpressionAndParam(auditRecords, expression, parameters)

        val result = auditApi!!.loadAudit(expression, parameters)

        Assert.assertEquals(result.toSet(), setOf(
                fullAuditRecord(*testSetFirst, unixTimeStamp = testStampFirst),
                fullAuditRecord(*testSetSecond, unixTimeStamp = testStampSecond)))
    }

    @Test
    fun loadAudit_UnknownAuditType_EmptyListReturned() {
        addPrimitiveTypes()
        val expression = createExpressionString()
        val parameters = createSimpleParam()
        throwUnknownAuditTypeOnExpressionAndParam(expression, parameters)

        val result = auditApi!!.loadAudit(expression, parameters)

        Assert.assertEquals(result, emptyList<AuditRecordInternal>())
    }

    @Test
    fun loadAuditWithExceptions_recordLoaded_AppropriateAuditRecordReturned() {
        val testSet = arrayOf("123", 456, TestClassFirst())
        val testStamp = 789L
        addPrimitiveTypesAndTestClassFirst()
        val auditRecord = createAuditRecordInternal(*testSet, unixTimeStamp = testStamp)
        val expression = createExpressionString()
        val parameters = createSimpleParam()
        returnRecordOnExpressionAndParam(auditRecord, expression, parameters)

        val result = auditApi!!.loadAuditWithExceptions(expression, parameters)

        Assert.assertEquals(result, listOf(fullAuditRecord(*testSet, unixTimeStamp = testStamp)))
    }

    @Test
    fun loadAuditWithExceptions_recordsLoaded_AppropriateAuditRecordsReturned() {
        val testSetFirst = arrayOf("123", 456, TestClassFirst())
        val testStampFirst = 1L
        val testSetSecond = arrayOf("123", 789)
        val testStampSecond = 2L
        addPrimitiveTypesAndTestClassFirst()
        val auditRecords = listOf(
                createAuditRecordInternal(*testSetFirst, unixTimeStamp = testStampFirst),
                createAuditRecordInternal(*testSetSecond, unixTimeStamp = testStampSecond)
        )
        val expression = createExpressionString()
        val parameters = createSimpleParam()
        returnRecordsOnExpressionAndParam(auditRecords, expression, parameters)

        val result = auditApi!!.loadAuditWithExceptions(expression, parameters)

        Assert.assertEquals(result.toSet(), setOf(
                fullAuditRecord(*testSetFirst, unixTimeStamp = testStampFirst),
                fullAuditRecord(*testSetSecond, unixTimeStamp = testStampSecond)))
    }

    @Test
    fun loadAuditWithExceptions_UnknownAuditType_ExceptionThrown() {
        addPrimitiveTypes()
        val expression = createExpressionString()
        val parameters = createSimpleParam()
        throwUnknownAuditTypeOnExpressionAndParam(expression, parameters)

        try {
            auditApi!!.loadAuditWithExceptions(expression, parameters)
        } catch (e: UnknownAuditTypeException) {
            return
        }
        Assert.fail()
    }

    private fun createAuditRecordInternal(vararg objects: Any, unixTimeStamp: Long): AuditRecordInternal {
        return AuditRecordInternal(*objects, unixTimeStamp = unixTimeStamp)
    }

    private fun fullAuditRecord(vararg objects: Any, unixTimeStamp: Long): AuditRecord {
        val auditObjects = objects.map { o -> AuditType.resolveType(o::class).let { AuditObject(it, it.display(o), o) } };
        return AuditRecord(auditObjects, unixTimeStamp)
    }

    private fun createExpressionString(): QueryExpression {
        return String::class equal "123"
    }

    private fun createSimpleParam(): QueryParameters {
        return QueryParameters()
    }

    private fun returnRecordOnExpressionAndParam(record: AuditRecordInternal, expression: QueryExpression,
                                                 parameters: QueryParameters) {
        PowerMockito.`when`(auditDao!!.loadRecords(expression, parameters)).thenReturn(listOf(record))
    }

    private fun throwUnknownAuditTypeOnExpressionAndParam(expression: QueryExpression, parameters: QueryParameters) {
        PowerMockito.`when`(auditDao!!.loadRecords(expression, parameters)).thenThrow(UnknownAuditTypeException::class.java)
    }

    private fun returnRecordsOnExpressionAndParam(records: List<AuditRecordInternal>, expression: QueryExpression,
                                                  parameters: QueryParameters) {
        PowerMockito.`when`(auditDao!!.loadRecords(expression, parameters)).thenReturn(records)
    }

    private fun addPrimitiveTypes() {
        auditApi!!.addPrimitiveTypes()
    }

    private fun addPrimitiveTypesAndTestClassFirst() {
        auditApi!!.addPrimitiveTypes()
        val type = AuditType(TestClassFirst::class, "TestClassFirst", TestClassFirst)
        auditApi!!.addTypeForAudit(type)
    }
}
