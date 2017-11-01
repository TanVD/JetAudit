//package Clickhouse.AuditDao.Loading
//
//import org.testng.Assert
//import org.testng.annotations.AfterMethod
//import org.testng.annotations.BeforeMethod
//import org.testng.annotations.Test
//import tanvd.audit.implementation.clickhouse.AuditDaoClickhouseImpl
//import tanvd.audit.implementation.dao.AuditDao
//import tanvd.audit.model.external.presenters.StringPresenter
//import tanvd.audit.model.external.queries.not
//import tanvd.audit.model.external.records.InformationObject
//import utils.DbUtils
//import utils.InformationUtils
//import utils.SamplesGenerator
//import utils.TypeUtils
//
//internal class UnaryQueryOperatorsTest {
//
//    companion object {
//        var currentId = 0L
//        var auditDao: AuditDaoClickhouseImpl? = null
//    }
//
//
//    @BeforeMethod
//    @Suppress("UNCHECKED_CAST")
//    fun createAll() {
//        TypeUtils.addObjectTypePrimitives()
//        TypeUtils.addInformationTypesPrimitive()
//
//        AuditDao.credentials = DbUtils.getCredentials()
//        auditDao = AuditDao.getDao() as AuditDaoClickhouseImpl
//
//        TypeUtils.addAuditTypePrimitive(auditDao!!)
//    }
//
//    @AfterMethod
//    fun clearAll() {
//        auditDao!!.dropTable(AuditDaoClickhouseImpl.auditTable)
//        TypeUtils.clearTypes()
//        currentId = 0
//    }
//
//    @Test
//    fun loadRows_NotStringsEqual_loadedBoth() {
//        val auditRecordFirstOriginal = SamplesGenerator.getRecordInternal("string1", "string2", information = getSampleInformation())
//        val auditRecordSecondOriginal = SamplesGenerator.getRecordInternal("string1", "string2", information = getSampleInformation())
//        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal, auditRecordSecondOriginal))
//
//        val recordsLoaded = auditDao!!.loadRecords(
//                not(StringPresenter.value equal "string3"), QueryParameters())
//        Assert.assertEquals(recordsLoaded.toSet(), setOf(auditRecordFirstOriginal, auditRecordSecondOriginal))
//
//    }
//
//    @Test
//    fun loadRows_NotStringsEqual_loadedOne() {
//        val auditRecordFirstOriginal = SamplesGenerator.getRecordInternal("string1", "string2", information = getSampleInformation())
//        val auditRecordSecondOriginal = SamplesGenerator.getRecordInternal("string1", "string1", information = getSampleInformation())
//        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal, auditRecordSecondOriginal))
//
//        val recordsLoaded = auditDao!!.loadRecords(
//                not(StringPresenter.value equal "string2"), QueryParameters())
//        Assert.assertEquals(recordsLoaded, listOf(auditRecordSecondOriginal))
//
//    }
//
//    @Test
//    fun loadRows_NotStringsEqual_loadedNone() {
//        val auditRecordFirstOriginal = SamplesGenerator.getRecordInternal("string1", "string1", information = getSampleInformation())
//        val auditRecordSecondOriginal = SamplesGenerator.getRecordInternal("string1", "string1", information = getSampleInformation())
//        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal, auditRecordSecondOriginal))
//
//        val recordsLoaded = auditDao!!.loadRecords(
//                not(StringPresenter.value equal "string1"), QueryParameters())
//        Assert.assertEquals(recordsLoaded.size, 0)
//    }
//
//    private fun getSampleInformation(): MutableSet<InformationObject<*>> {
//        return InformationUtils.getPrimitiveInformation(currentId++, 1, 2, SamplesGenerator.getMillenniumStart())
//    }
//}