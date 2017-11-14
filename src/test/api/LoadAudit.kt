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
import tanvd.aorm.query.LimitExpression
import tanvd.aorm.query.OrderByExpression
import tanvd.aorm.query.QueryExpression
import tanvd.audit.AuditAPI
import tanvd.audit.exceptions.UnknownObjectTypeException
import tanvd.audit.implementation.AuditExecutor
import tanvd.audit.implementation.QueueCommand
import tanvd.audit.implementation.clickhouse.AuditDao
import tanvd.audit.implementation.clickhouse.AuditDaoClickhouse
import tanvd.audit.model.external.presenters.StringPresenter
import tanvd.audit.model.external.queries.equal
import tanvd.audit.model.external.records.AuditObject
import tanvd.audit.model.external.records.AuditRecord
import tanvd.audit.model.external.records.InformationObject
import tanvd.audit.model.external.types.objects.ObjectType
import tanvd.audit.model.internal.AuditRecordInternal
import utils.*
import utils.SamplesGenerator.getRecordInternal
import java.util.*
import java.util.concurrent.BlockingQueue
import kotlin.collections.LinkedHashSet

@PowerMockIgnore("javax.management.*", "javax.xml.parsers.*", "com.sun.org.apache.xerces.internal.jaxp.*", "ch.qos.logback.*", "org.slf4j.*")
@PrepareForTest(AuditExecutor::class, ObjectType::class)
internal class LoadAudit : PowerMockTestCase() {

    private var auditDao: AuditDao? = null

    private var auditExecutor: AuditExecutor? = null

    private var auditQueueInternal: BlockingQueue<QueueCommand>? = null

    private var auditRecordsNotCommitted: ThreadLocal<ArrayList<AuditRecordInternal>>? = null

    private var auditApi: AuditAPI? = null

    @BeforeClass
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
        auditApi = AuditAPI(auditDao!!, auditExecutor!!, auditQueueInternal!!, auditRecordsNotCommitted!!, DbUtils.getProperties())
    }

    @AfterMethod
    fun resetMocks() {
        auditRecordsNotCommitted!!.remove()
        Mockito.reset(auditDao)
        Mockito.reset(auditExecutor)
        Mockito.reset(auditQueueInternal)
        TestUtil.clearTypes()
    }

    @Test
    fun loadAudit_recordLoaded_AppropriateAuditRecordReturned() {
        val testSet = arrayOf("123", 456, TestClassString("string"))
        val testStamp = 789L
        addPrimitiveTypesAndTestClassFirst()
        val auditRecord = createAuditRecordInternal(*testSet, unixTimeStamp = testStamp, id = 1)
        val expression = createExpressionString()
        returnRecordOnExpressionAndParam(auditRecord, expression)

        val result = auditApi!!.load(expression)

        Assert.assertEquals(result, listOf(fullAuditRecord(*testSet, unixTimeStamp = testStamp, id = 1)))
    }

    @Test
    fun loadAudit_recordsLoaded_AppropriateAuditRecordsReturned() {
        val testSetFirst = arrayOf("123", 456, TestClassString("string"))
        val testStampFirst = 1L
        val testSetSecond = arrayOf("123", 789)
        val testStampSecond = 2L
        addPrimitiveTypesAndTestClassFirst()
        val auditRecords = listOf(
                createAuditRecordInternal(*testSetFirst, unixTimeStamp = testStampFirst, id = 1),
                createAuditRecordInternal(*testSetSecond, unixTimeStamp = testStampSecond, id = 2)
        )
        val expression = createExpressionString()
        returnRecordsOnExpressionAndParam(auditRecords, expression)

        val result = auditApi!!.load(expression)

        Assert.assertEquals(result.toSet(), setOf(
                fullAuditRecord(*testSetFirst, unixTimeStamp = testStampFirst, id = 1),
                fullAuditRecord(*testSetSecond, unixTimeStamp = testStampSecond, id = 2)))
    }

    @Test
    fun loadAudit_UnknownAuditType_EmptyListReturned() {
        addPrimitiveTypes()
        val expression = createExpressionString()
        throwUnknownAuditTypeOnExpressionAndParam(expression)

        val result = auditApi!!.load(expression)

        Assert.assertEquals(result, emptyList<AuditRecordInternal>())
    }

    @Test
    fun loadAuditWithExceptions_recordLoaded_AppropriateAuditRecordReturned() {
        val testSet = arrayOf("123", 456, TestClassString("string"))
        val testStamp = 789L
        addPrimitiveTypesAndTestClassFirst()
        val auditRecord = createAuditRecordInternal(*testSet, unixTimeStamp = testStamp, id = 1)
        val expression = createExpressionString()
        returnRecordOnExpressionAndParam(auditRecord, expression)

        val result = auditApi!!.loadAuditWithExceptions(expression)

        Assert.assertEquals(result, listOf(fullAuditRecord(*testSet, unixTimeStamp = testStamp, id = 1)))
    }

    @Test
    fun loadAuditWithExceptions_recordsLoaded_AppropriateAuditRecordsReturned() {
        val testSetFirst = arrayOf("123", 456, TestClassString("string"))
        val testStampFirst = 1L
        val testSetSecond = arrayOf("123", 789)
        val testStampSecond = 2L
        addPrimitiveTypesAndTestClassFirst()
        val auditRecords = listOf(
                createAuditRecordInternal(*testSetFirst, unixTimeStamp = testStampFirst, id = 1),
                createAuditRecordInternal(*testSetSecond, unixTimeStamp = testStampSecond, id = 2)
        )
        val expression = createExpressionString()
        returnRecordsOnExpressionAndParam(auditRecords, expression)

        val result = auditApi!!.loadAuditWithExceptions(expression)

        Assert.assertEquals(result.toSet(), setOf(
                fullAuditRecord(*testSetFirst, unixTimeStamp = testStampFirst, id = 1),
                fullAuditRecord(*testSetSecond, unixTimeStamp = testStampSecond, id = 2)))
    }

    @Test
    fun loadAuditWithExceptions_UnknownAuditType_ExceptionThrown() {
        addPrimitiveTypes()
        val expression = createExpressionString()
        throwUnknownAuditTypeOnExpressionAndParam(expression)

        try {
            auditApi!!.loadAuditWithExceptions(expression)
        } catch (e: UnknownObjectTypeException) {
            return
        }
        Assert.fail()
    }

    private fun createAuditRecordInternal(vararg objects: Any, unixTimeStamp: Long, id: Long): AuditRecordInternal {
        return getRecordInternal(*objects, information = getSampleInformation(unixTimeStamp, id))
    }

    private fun fullAuditRecord(vararg objects: Any, unixTimeStamp: Long, id: Long): AuditRecord {
        val auditObjects = objects.map { o -> ObjectType.resolveType(o::class).let { AuditObject(it, o, it.serialize(o)) } }
        return AuditRecord(auditObjects, getSampleInformation(unixTimeStamp, id))
    }

    private fun createExpressionString(): QueryExpression {
        return StringPresenter.value equal "123"
    }

    private fun returnRecordOnExpressionAndParam(record: AuditRecordInternal, expression: QueryExpression,
                                                 limit: LimitExpression? = null, orderBy: OrderByExpression? = null) {
        PowerMockito.`when`(auditDao!!.loadRecords(expression, limit, orderBy)).thenReturn(listOf(record))
    }

    private fun throwUnknownAuditTypeOnExpressionAndParam(expression: QueryExpression, limit: LimitExpression? = null,
                                                          orderBy: OrderByExpression? = null) {
        PowerMockito.`when`(auditDao!!.loadRecords(expression, limit, orderBy)).thenThrow(UnknownObjectTypeException::class.java)
    }

    private fun returnRecordsOnExpressionAndParam(records: List<AuditRecordInternal>, expression: QueryExpression,
                                                  limit: LimitExpression? = null, orderBy: OrderByExpression? = null) {
        PowerMockito.`when`(auditDao!!.loadRecords(expression, limit, orderBy)).thenReturn(records)
    }

    private fun addPrimitiveTypes() {
        auditApi!!.addPrimitiveTypes()
        auditApi!!.addServiceInformation()
    }

    private fun addPrimitiveTypesAndTestClassFirst() {
        auditApi!!.addPrimitiveTypes()
        auditApi!!.addServiceInformation()
        val type = ObjectType(TestClassString::class, TestClassStringPresenter)
        auditApi!!.addObjectType(type)
    }

    private fun getSampleInformation(timeStamp: Long, id: Long, isDeleted: Boolean = false): LinkedHashSet<InformationObject<*>> {
        return InformationUtils.getPrimitiveInformation(id, timeStamp, 2, SamplesGenerator.getMillenniumStart(), isDeleted)
    }
}
