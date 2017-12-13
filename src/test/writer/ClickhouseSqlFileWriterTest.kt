package writer

import org.mockito.Mockito
import org.powermock.api.mockito.PowerMockito
import org.powermock.core.classloader.annotations.PowerMockIgnore
import org.powermock.modules.testng.PowerMockTestCase
import org.testng.annotations.AfterMethod
import org.testng.annotations.BeforeMethod
import org.testng.annotations.Test
import tanvd.audit.implementation.clickhouse.aorm.AuditTable
import tanvd.audit.implementation.writer.ClickhouseSqlFileWriter
import tanvd.audit.model.external.presenters.*
import tanvd.audit.model.external.records.InformationObject
import utils.InformationUtils
import utils.SamplesGenerator
import utils.SamplesGenerator.getRecordInternal
import utils.TestUtil
import java.io.PrintWriter

@PowerMockIgnore("javax.management.*", "javax.xml.parsers.*", "com.sun.org.apache.xerces.internal.jaxp.*", "ch.qos.logback.*", "org.slf4j.*")
internal class ClickhouseSqlFileWriterTest : PowerMockTestCase() {

    @BeforeMethod
    fun init() {
        TestUtil.create()
    }

    @AfterMethod
    fun clean() {
        TestUtil.drop()
    }

    @Test
    fun write_gotAuditRecordInternal_AppropriateSqlInsertWritten() {
        val id = 0L
        val version = 1L
        val timeStamp = 2L
        val auditRecord = getRecordInternal(123, "456", information = getSampleInformation(id, timeStamp, version))
        val fileWriter = PowerMockito.mock(PrintWriter::class.java)
        val reserveWriter = ClickhouseSqlFileWriter(fileWriter)

        reserveWriter.write(auditRecord)

        Mockito.verify(fileWriter).println("INSERT INTO ${AuditTable().name} (" +
                "${IntPresenter.value.column.name}, ${StringPresenter.value.column.name}, " +
                "${IdType.column.name}, " +
                "${TimeStampType.column.name}, " +
                "${VersionType.column.name}, " +
                "${DateType.column.name}, " +
                "${AuditTable().description.name}, " +
                "${LongPresenter.value.column.name}) VALUES " +
                "([123], ['456'], 0, 2, 1, '2000-01-01', ['Int', 'String'], []);")
    }

    @Test
    fun flush_gotFlush_printWriterFlushed() {
        val fileWriter = PowerMockito.mock(PrintWriter::class.java)
        val reserveWriter = ClickhouseSqlFileWriter(fileWriter)

        reserveWriter.flush()

        Mockito.verify(fileWriter).flush()
    }

    private fun getSampleInformation(id: Long, timeStamp: Long, version: Long): LinkedHashSet<InformationObject<*>> {
        return InformationUtils.getPrimitiveInformation(id, timeStamp, version, SamplesGenerator.getMillenniumStart())
    }
}