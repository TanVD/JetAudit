//package writer
//
//import org.mockito.Mockito
//import org.powermock.api.mockito.PowerMockito
//import org.powermock.core.classloader.annotations.PowerMockIgnore
//import org.powermock.modules.testng.PowerMockTestCase
//import org.testng.annotations.AfterMethod
//import org.testng.annotations.BeforeMethod
//import org.testng.annotations.Test
//import tanvd.audit.implementation.clickhouse.AuditDaoClickhouseImpl
//import tanvd.audit.implementation.clickhouse.model.getCode
//import tanvd.audit.implementation.writer.ClickhouseSqlFileWriter
//import tanvd.audit.model.external.presenters.*
//import tanvd.audit.model.external.records.InformationObject
//import tanvd.audit.model.external.types.information.InformationType
//import utils.InformationUtils
//import utils.SamplesGenerator
//import utils.SamplesGenerator.getRecordInternal
//import utils.TypeUtils
//import java.io.PrintWriter
//
//@PowerMockIgnore("javax.management.*", "javax.xml.parsers.*", "com.sun.org.apache.xerces.internal.jaxp.*", "ch.qos.logback.*", "org.slf4j.*")
//internal class ClickhouseSqlFileWriterTest : PowerMockTestCase() {
//
//    @BeforeMethod
//    fun init() {
//        TypeUtils.addObjectTypePrimitives()
//        TypeUtils.addInformationTypesPrimitive()
//    }
//
//    @AfterMethod
//    fun clean() {
//        TypeUtils.clearTypes()
//    }
//
//    @Test
//    fun write_gotAuditRecordInternal_AppropriateSqlInsertWritten() {
//        val id = 0L
//        val version = 1L
//        val timeStamp = 2L
//        val auditRecord = getRecordInternal(123, "456", information = getSampleInformation(id, timeStamp, version))
//        val fileWriter = PowerMockito.mock(PrintWriter::class.java)
//        val reserveWriter = ClickhouseSqlFileWriter(fileWriter)
//
//        reserveWriter.write(auditRecord)
//
//        Mockito.verify(fileWriter).println("INSERT INTO ${AuditDaoClickhouseImpl.auditTable} (" +
//                "${IntPresenter.value.getCode()}, ${StringPresenter.value.getCode()}, ${AuditDaoClickhouseImpl.descriptionColumn}, " +
//                "${InformationType.resolveType(IdType).code}, " +
//                "${InformationType.resolveType(VersionType).code}, " +
//                "${InformationType.resolveType(TimeStampType).code}, " +
//                "${InformationType.resolveType(DateType).code}, " +
//                "${InformationType.resolveType(IsDeletedType).code}) VALUES " +
//                "([123], ['456'], ['Int', 'String'], 0, 1, 2, '2000-01-01', 0);")
//    }
//
//    @Test
//    fun flush_gotFlush_printWriterFlushed() {
//        val fileWriter = PowerMockito.mock(PrintWriter::class.java)
//        val reserveWriter = ClickhouseSqlFileWriter(fileWriter)
//
//        reserveWriter.flush()
//
//        Mockito.verify(fileWriter).flush()
//    }
//
//    private fun getSampleInformation(id: Long, timeStamp: Long, version: Long): MutableSet<InformationObject<*>> {
//        return InformationUtils.getPrimitiveInformation(id, timeStamp, version, SamplesGenerator.getMillenniumStart())
//    }
//}