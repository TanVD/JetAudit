package Clickhouse.AuditDao.Loading.Objects

import javassist.bytecode.SignatureAttribute
import org.testng.Assert
import org.testng.annotations.AfterMethod
import org.testng.annotations.BeforeMethod
import org.testng.annotations.Test
import tanvd.audit.implementation.clickhouse.AuditDaoClickhouseImpl
import tanvd.audit.implementation.dao.DbType
import tanvd.audit.model.external.queries.QueryParameters
import tanvd.audit.model.external.queries.`is`
import tanvd.audit.model.external.queries.isNot
import tanvd.audit.model.external.records.InformationObject
import tanvd.audit.model.external.types.InnerType
import tanvd.audit.model.external.types.information.InformationType
import tanvd.audit.model.external.types.objects.ObjectType
import tanvd.audit.model.internal.AuditRecordInternal
import utils.*
import utils.SamplesGenerator.getRecordInternal

internal class StateBooleanTypeTest {

    companion object {
        var currentId = 0L
        var auditDao: AuditDaoClickhouseImpl? = null
    }

    @BeforeMethod
    @Suppress("UNCHECKED_CAST")
    fun createAll() {
        TypeUtils.addAuditTypesPrimitive()
        TypeUtils.addInformationTypesPrimitive()

        auditDao = DbType.Clickhouse.getDao("jdbc:clickhouse://localhost:8123/example", "default", "") as AuditDaoClickhouseImpl

        TypeUtils.addAuditTypePrimitive(auditDao!!)

        val type = ObjectType(TestClassBoolean::class, TestClassBooleanPresenter) as ObjectType<Any>
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
    fun loadRow_LoadByIs_loadedOne() {
        val auditRecordFirstOriginal = getRecordInternal(TestClassBoolean(true), information = getSampleInformation())

        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal))

        val recordsLoaded = auditDao!!.loadRecords(TestClassBooleanPresenter.id `is` true, QueryParameters())
        Assert.assertEquals(recordsLoaded, listOf(auditRecordFirstOriginal))
    }

    @Test
    fun loadRow_LoadByIs_loadedNone() {
        val auditRecordFirstOriginal = getRecordInternal(TestClassBoolean(true), information = getSampleInformation())

        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal))

        val recordsLoaded = auditDao!!.loadRecords(TestClassBooleanPresenter.id `is` false, QueryParameters())
        Assert.assertEquals(recordsLoaded.size, 0)
    }

    @Test
    fun loadRow_LoadByIsNot_loadedOne() {
        val auditRecordFirstOriginal = getRecordInternal(TestClassBoolean(true), information = getSampleInformation())

        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal))

        val recordsLoaded = auditDao!!.loadRecords(TestClassBooleanPresenter.id isNot false, QueryParameters())
        Assert.assertEquals(recordsLoaded, listOf(auditRecordFirstOriginal))
    }

    @Test
    fun loadRow_LoadByIsNot_loadedNone() {
        val auditRecordFirstOriginal = getRecordInternal(TestClassBoolean(true), information = getSampleInformation())

        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal))

        val recordsLoaded = auditDao!!.loadRecords(TestClassBooleanPresenter.id isNot true, QueryParameters())
        Assert.assertEquals(recordsLoaded.size, 0)
    }

    private fun getSampleInformation(): MutableSet<InformationObject> {
        return InformationUtils.getPrimitiveInformation(currentId++, 1, 2)

    }
}
