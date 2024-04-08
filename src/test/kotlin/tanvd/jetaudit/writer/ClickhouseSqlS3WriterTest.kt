package tanvd.jetaudit.writer

import org.junit.*
import org.junit.runner.RunWith
import org.mockito.Mockito
import org.powermock.core.classloader.annotations.PowerMockIgnore
import org.powermock.modules.junit4.PowerMockRunner
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.PutObjectRequest
import tanvd.jetaudit.implementation.clickhouse.aorm.AuditTable
import tanvd.jetaudit.implementation.writer.ClickhouseSqlS3Writer
import tanvd.jetaudit.model.external.presenters.*
import tanvd.jetaudit.model.external.records.InformationObject
import tanvd.jetaudit.utils.*

@RunWith(PowerMockRunner::class)
@PowerMockIgnore("javax.management.*", "javax.xml.parsers.*", "javax.net.*",
        "jdk.*",
        "com.sun.org.apache.xerces.internal.jaxp.*", "ch.qos.logback.*",
        "org.slf4j.*")
internal class ClickhouseSqlS3WriterTest {

    @Before
    fun init() {
        TestUtil.create()
    }

    @After
    fun clean() {
        TestUtil.drop()
    }

    @Test
    fun write_gotAuditRecordInternal_AppropriateSqlInsertWritten() {
        val id = 0L
        val version = 1L
        val timeStamp = 2L
        val auditRecord = SamplesGenerator.getRecordInternal(123, "456", information = getSampleInformation(id, timeStamp, version))
        val s3Client = Mockito.mock(S3Client::class.java)
        val reserveWriter = ClickhouseSqlS3Writer(s3Client)

        reserveWriter.write(auditRecord)
        reserveWriter.flush()

        val content = "INSERT INTO ${AuditTable.name} (" +
                "${IntPresenter.value.column.name}, ${StringPresenter.value.column.name}, " +
                "${IdType.column.name}, " +
                "${TimeStampType.column.name}, " +
                "${VersionType.column.name}, " +
                "${DateType.column.name}, " +
                "${IsDeletedType.column.name}, " +
                "${AuditTable.description.name}, " +
                "${LongPresenter.value.column.name}) VALUES " +
                "([123], ['456'], 0, 2, 1, '2000-01-01', 0, ['Int', 'String'], [])"
        val body = RequestBody.fromString(content)

        Mockito.verify(s3Client).putObject(
            Mockito.argThat<PutObjectRequest> {it.bucket() == "ClickhouseFailover" },
            Mockito.argThat<RequestBody> { it.contentStreamProvider().newStream()
                .readBytes().toString(Charsets.UTF_8) == content }
        )
    }

    private fun getSampleInformation(id: Long, timeStamp: Long, version: Long): LinkedHashSet<InformationObject<*>> {
        return InformationUtils.getPrimitiveInformation(id, timeStamp, version, SamplesGenerator.getMillenniumStart())
    }
}
