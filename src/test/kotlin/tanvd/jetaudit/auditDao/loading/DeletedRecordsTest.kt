package tanvd.jetaudit.auditDao.loading

import org.junit.*
import tanvd.jetaudit.implementation.clickhouse.AuditDaoClickhouse
import tanvd.jetaudit.model.external.equal
import tanvd.jetaudit.model.external.records.InformationObject
import tanvd.jetaudit.model.external.types.objects.ObjectType
import tanvd.jetaudit.model.internal.AuditRecordInternal
import tanvd.jetaudit.utils.*
import kotlin.properties.Delegates

@Suppress("UNCHECKED_CAST")
internal class DeletedRecordsTest {

    companion object {
        var currentId = 0L
        var auditDao: AuditDaoClickhouse by Delegates.notNull()
    }

    val type = ObjectType(TestClassString::class, TestClassStringPresenter) as ObjectType<Any>

    @Before
    @Suppress("UNCHECKED_CAST")
    fun createAll() {
        auditDao = TestUtil.create()

        ObjectType.addType(type)
        auditDao.addTypeInDbModel(type)
    }

    @After
    fun clearAll() {
        TestUtil.drop()
        currentId = 0
    }

    @Test
    fun loadRow_recordInsertedAsDeleted_recordNotLoaded() {
        val auditRecordFirstOriginal = SamplesGenerator.getRecordInternal(TestClassString("string1"),
                information = getSampleInformation(true))
        val auditRecordSecondOriginal = SamplesGenerator.getRecordInternal(TestClassString("string2"),
                information = getSampleInformation(false))

        auditDao.saveRecords(listOf(auditRecordFirstOriginal, auditRecordSecondOriginal))

        val recordsNotLoaded = auditDao.loadRecords(TestClassStringPresenter.id equal "string1")
        Assert.assertTrue(recordsNotLoaded.isEmpty())
        val recordsLoaded = auditDao.loadRecords(TestClassStringPresenter.id equal "string2")
        Assert.assertTrue(recordsLoaded.isNotEmpty())
    }

    @Test
    fun loadRow_recordDeleted_recordNotLoaded() {
        val auditRecordFirstOriginal = SamplesGenerator.getRecordInternal(TestClassString("string1"),
                information = getSampleInformation(false))

        auditDao.saveRecord(auditRecordFirstOriginal)

        val recordsLoaded = auditDao.loadRecords(TestClassStringPresenter.id equal "string1")
        Assert.assertTrue(recordsLoaded.isNotEmpty())

        auditDao.saveRecord(AuditRecordInternal(auditRecordFirstOriginal.objects, getSampleInformation(true)))

        val recordsNotLoaded = auditDao.loadRecords(TestClassStringPresenter.id equal "string1")
        Assert.assertTrue(recordsNotLoaded.isEmpty())
    }

    @Test
    fun loadRow_recordNotDeleted_recordLoaded() {
        val auditRecordFirstOriginal = SamplesGenerator.getRecordInternal(TestClassString("string1"),
                information = getSampleInformation(false))
        val auditRecordSecondOriginal = SamplesGenerator.getRecordInternal(TestClassString("string2"),
                information = getSampleInformation(false))

        auditDao.saveRecords(listOf(auditRecordFirstOriginal, auditRecordSecondOriginal))

        val recordsLoaded = auditDao.loadRecords(TestClassStringPresenter.id equal "string2")
        Assert.assertEquals(recordsLoaded.single(), auditRecordSecondOriginal)
    }

    private fun getSampleInformation(isDeleted: Boolean): LinkedHashSet<InformationObject<*>> {
        val (id, version) = if (isDeleted) (currentId to 3L) else ++currentId to 2L
        return InformationUtils.getPrimitiveInformation(id, 1, version,
                SamplesGenerator.getMillenniumStart(), isDeleted)
    }
}
