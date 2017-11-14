package clickhouse.auditDao.loading

import clickhouse.auditDao.loading.objects.StateBooleanTypeTest
import org.testng.Assert
import org.testng.annotations.AfterMethod
import org.testng.annotations.BeforeMethod
import org.testng.annotations.Test
import tanvd.audit.implementation.clickhouse.AuditDaoClickhouse
import tanvd.audit.model.external.queries.equal
import tanvd.audit.model.external.records.InformationObject
import tanvd.audit.model.external.types.objects.ObjectType
import utils.*

@Suppress("UNCHECKED_CAST")
internal class DeletedRecordsTest {

    companion object {
        var currentId = 0L
        var auditDao: AuditDaoClickhouse? = null
    }

    val type = ObjectType(TestClassString::class, TestClassStringPresenter) as ObjectType<Any>

    @BeforeMethod
    @Suppress("UNCHECKED_CAST")
    fun createAll() {
        auditDao = TestUtil.create()

        ObjectType.addType(type)
        auditDao!!.addTypeInDbModel(type)
    }

    @AfterMethod
    fun clearAll() {
        TestUtil.drop()
        currentId = 0
    }

    @Test
    fun loadRow_recordDeleted_recordNotLoaded() {
        val auditRecordFirstOriginal = SamplesGenerator.getRecordInternal(TestClassString("string1"),
                information = getSampleInformation(true))
        val auditRecordSecondOriginal = SamplesGenerator.getRecordInternal(TestClassString("string2"),
                information = getSampleInformation(false))

        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal, auditRecordSecondOriginal))

        val recordsLoaded = auditDao!!.loadRecords(TestClassStringPresenter.id equal "string1")
        Assert.assertTrue(recordsLoaded.isEmpty())
    }

    @Test
    fun loadRow_recordNotDeleted_recordLoaded() {
        val auditRecordFirstOriginal = SamplesGenerator.getRecordInternal(TestClassString("string1"),
                information = getSampleInformation(true))
        val auditRecordSecondOriginal = SamplesGenerator.getRecordInternal(TestClassString("string2"),
                information = getSampleInformation(false))

        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal, auditRecordSecondOriginal))

        val recordsLoaded = auditDao!!.loadRecords(TestClassStringPresenter.id equal "string2")
        Assert.assertEquals(recordsLoaded.single(), auditRecordSecondOriginal)
    }

    private fun getSampleInformation(isDeleted: Boolean): LinkedHashSet<InformationObject<*>> {
        return InformationUtils.getPrimitiveInformation(StateBooleanTypeTest.currentId++, 1, 2,
                SamplesGenerator.getMillenniumStart(), isDeleted)
    }
}