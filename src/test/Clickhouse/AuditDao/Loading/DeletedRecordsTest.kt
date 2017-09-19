package Clickhouse.AuditDao.Loading

import Clickhouse.AuditDao.Loading.Objects.StateBooleanTypeTest
import org.testng.Assert
import org.testng.annotations.AfterMethod
import org.testng.annotations.BeforeMethod
import org.testng.annotations.Test
import tanvd.audit.implementation.clickhouse.AuditDaoClickhouseImpl
import tanvd.audit.implementation.dao.AuditDao
import tanvd.audit.model.external.queries.QueryParameters
import tanvd.audit.model.external.queries.equal
import tanvd.audit.model.external.records.InformationObject
import tanvd.audit.model.external.types.objects.ObjectType
import utils.*

internal class DeletedRecordsTest {

    companion object {
        var currentId = 0L
        var auditDao: AuditDaoClickhouseImpl? = null
    }

    @BeforeMethod
    @Suppress("UNCHECKED_CAST")
    fun createAll() {
        TypeUtils.addAuditTypesPrimitive()
        TypeUtils.addInformationTypesPrimitive()

        AuditDao.credentials = DbUtils.getCredentials()
        auditDao = AuditDao.getDao() as AuditDaoClickhouseImpl

        TypeUtils.addAuditTypePrimitive(auditDao!!)

        val type = ObjectType(TestClassString::class, TestClassStringPresenter) as ObjectType<Any>
        ObjectType.addType(type)
        auditDao!!.addTypeInDbModel(type)
    }

    @AfterMethod
    fun clearAll() {
        auditDao!!.dropTable(AuditDaoClickhouseImpl.auditTable)
        TypeUtils.clearTypes()
        currentId = 0
    }

    @Test
    fun loadRow_recordDeleted_recordNotLoaded() {
        val auditRecordFirstOriginal = SamplesGenerator.getRecordInternal(TestClassString("string1"),
                information = getSampleInformation(true))
        val auditRecordSecondOriginal = SamplesGenerator.getRecordInternal(TestClassString("string2"),
                information = getSampleInformation(false))

        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal, auditRecordSecondOriginal))

        val recordsLoaded = auditDao!!.loadRecords(TestClassStringPresenter.id equal "string1", QueryParameters())
        Assert.assertTrue(recordsLoaded.isEmpty())
    }

    @Test
    fun loadRow_recordNotDeleted_recordLoaded() {
        val auditRecordFirstOriginal = SamplesGenerator.getRecordInternal(TestClassString("string1"),
                information = getSampleInformation(true))
        val auditRecordSecondOriginal = SamplesGenerator.getRecordInternal(TestClassString("string2"),
                information = getSampleInformation(false))

        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal, auditRecordSecondOriginal))

        val recordsLoaded = auditDao!!.loadRecords(TestClassStringPresenter.id equal "string2", QueryParameters())
        Assert.assertEquals(recordsLoaded.single(), auditRecordSecondOriginal)
    }

    private fun getSampleInformation(isDeleted: Boolean): MutableSet<InformationObject<*>> {
        return InformationUtils.getPrimitiveInformation(StateBooleanTypeTest.currentId++, 1, 2,
                SamplesGenerator.getMillenniumStart(), isDeleted)
    }
}