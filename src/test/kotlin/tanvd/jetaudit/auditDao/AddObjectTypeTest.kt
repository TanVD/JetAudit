package tanvd.jetaudit.auditDao

import org.junit.*
import tanvd.jetaudit.implementation.clickhouse.AuditDaoClickhouse
import tanvd.jetaudit.model.external.equal
import tanvd.jetaudit.model.external.presenters.StringPresenter
import tanvd.jetaudit.model.external.records.InformationObject
import tanvd.jetaudit.model.external.types.objects.ObjectType
import tanvd.jetaudit.utils.*
import tanvd.jetaudit.utils.SamplesGenerator.getRecordInternal

internal class AddObjectTypeTest {
    companion object {
        var auditDao: AuditDaoClickhouse? = null
        var currentId = 0L

    }

    @Before
    fun createAll() {
        auditDao = TestUtil.create()
    }

    @After
    fun clearAll() {
        TestUtil.drop()
        currentId = 0
    }

    @Test
    fun saveRecordBefore_addType_loadRecordsReturnSavedRecord() {
        val auditRecordOriginal = getRecordInternal("string", information = getSampleInformation())
        auditDao!!.saveRecord(auditRecordOriginal)

        addTestClassStringType()

        val recordsLoaded = auditDao!!.loadRecords(StringPresenter.value equal "string")
        Assert.assertEquals(recordsLoaded, listOf(auditRecordOriginal))
    }

    @Test
    fun saveRecordBefore_addTypeAndSaveWithNewType_loadRecordsLoadedNotNew() {
        val auditRecordFirstOriginal = getRecordInternal("string", 123, information = getSampleInformation())
        auditDao!!.saveRecord(auditRecordFirstOriginal)

        addTestClassStringType()

        val auditRecordSecondOriginal = getRecordInternal(TestClassString("string"), information = getSampleInformation())
        auditDao!!.saveRecord(auditRecordSecondOriginal)

        val recordsLoaded = auditDao!!.loadRecords(StringPresenter.value equal "string")

        Assert.assertEquals(recordsLoaded, listOf(auditRecordFirstOriginal))
    }

    @Test
    fun saveRecordBefore_addTypeAndSaveWithNewType_loadRecordsLoadedNew() {
        val auditRecordFirstOriginal = getRecordInternal("string", 123, information = getSampleInformation())
        auditDao!!.saveRecord(auditRecordFirstOriginal)

        addTestClassStringType()

        val auditRecordSecondOriginal = getRecordInternal(TestClassString("string"), information = getSampleInformation())
        auditDao!!.saveRecord(auditRecordSecondOriginal)


        val recordsLoaded = auditDao!!.loadRecords(TestClassStringPresenter.id equal "string")
        Assert.assertEquals(recordsLoaded, listOf(auditRecordSecondOriginal))
    }

    @Test
    fun saveRecordBefore_addTypeAndSaveWithNewType_loadRecordsLoadedBoth() {
        val auditRecordFirstOriginal = getRecordInternal("string", 123, information = getSampleInformation())
        auditDao!!.saveRecord(auditRecordFirstOriginal)

        addTestClassStringType()

        val auditRecordSecondOriginal = getRecordInternal("string", TestClassString("string"), information = getSampleInformation())
        auditDao!!.saveRecord(auditRecordSecondOriginal)


        val recordsLoaded = auditDao!!.loadRecords(StringPresenter.value equal "string")
        Assert.assertEquals(recordsLoaded.toSet(), setOf(auditRecordFirstOriginal, auditRecordSecondOriginal))
    }

    private fun addTestClassStringType() {
        @Suppress("UNCHECKED_CAST")
        val typeTestClassFirst = ObjectType(TestClassString::class, TestClassStringPresenter) as ObjectType<Any>
        ObjectType.addType(typeTestClassFirst)
        auditDao!!.addTypeInDbModel(typeTestClassFirst)
    }

    private fun getSampleInformation(): LinkedHashSet<InformationObject<*>> {
        return InformationUtils.getPrimitiveInformation(currentId++, 1, 2, SamplesGenerator.getMillenniumStart())
    }
}
