@file:JvmName("ClickhouseSqlS3WriterKt")

package tanvd.jetaudit.implementation.writer


import org.joda.time.DateTime
import org.joda.time.DateTimeZone
import org.joda.time.format.DateTimeFormat
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.PutObjectRequest
import tanvd.aorm.InsertRow
import tanvd.aorm.insert.InsertExpression
import tanvd.jetaudit.implementation.clickhouse.ClickhouseRecordSerializer
import tanvd.jetaudit.implementation.clickhouse.aorm.AuditTable
import tanvd.jetaudit.model.internal.AuditRecordInternal
import tanvd.jetaudit.utils.Conf
import tanvd.jetaudit.utils.PropertyLoader

private const val DEFAULT_REGION = "eu-west-1"

internal class ClickhouseSqlS3Writer : AuditReserveWriter {

    private val bucketName by lazy { PropertyLoader[Conf.S3_FAILOVER] }

    private val awsRegion by lazy { PropertyLoader.tryGet(Conf.AWS_REGION) }

    private val formatter = DateTimeFormat.forPattern("yyyyMMdd_HHmmss_SSS")

    private val buffer = ArrayList<String>()

    private val s3Client: S3Client

    constructor() {
        s3Client = S3Client.builder()
            .credentialsProvider(DefaultCredentialsProvider.create())
            .region(Region.of(awsRegion?: DEFAULT_REGION))
            .build()
    }

    constructor(s3Client: S3Client) {
        this.s3Client = s3Client
    }

    override fun write(record: AuditRecordInternal) {
        val row = ClickhouseRecordSerializer.serialize(record)
        buffer.add(InsertExpression(AuditTable, InsertRow(row.toMutableMap())).toSql())
    }

    override fun flush() = close()

    override fun close() {
        if (buffer.isNotEmpty()) {
            val key = "Failover_" + formatter.print(DateTime.now().withZone(DateTimeZone.UTC))
            val putObjectRequest = PutObjectRequest.builder()
                .bucket(bucketName)
                .key(key)
                .build()
            val responseBody = s3Client.putObject(
                putObjectRequest,
                RequestBody.fromString(buffer.joinToString(separator = "\n")))
            buffer.clear()
        }
    }

}