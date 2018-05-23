package tanvd.audit.implementation.writer

import com.amazonaws.ClientConfiguration
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import org.joda.time.DateTime
import org.joda.time.DateTimeZone
import org.joda.time.format.DateTimeFormat
import tanvd.aorm.DbType
import tanvd.aorm.InsertExpression
import tanvd.aorm.Row
import tanvd.aorm.expression.Column
import tanvd.audit.implementation.clickhouse.ClickhouseRecordSerializer
import tanvd.audit.implementation.clickhouse.aorm.AuditTable
import tanvd.audit.model.internal.AuditRecordInternal
import tanvd.audit.utils.Conf
import tanvd.audit.utils.PropertyLoader

internal class ClickhouseSqlS3Writer : AuditReserveWriter {

    private val bucketName by lazy { PropertyLoader[Conf.S3_FAILOVER] }

    private val awsRegion by lazy { PropertyLoader.tryGet(Conf.AWS_REGION) }

    private val formatter = DateTimeFormat.forPattern("yyyyMMdd_HHmmss_SSS")

    private val buffer = ArrayList<String>()

    private val s3Client: AmazonS3

    constructor() {
        s3Client = AmazonS3ClientBuilder.standard()
                .withCredentials(DefaultAWSCredentialsProviderChain.getInstance())
                .withClientConfiguration(ClientConfiguration().withGzip(true))
                .apply {
                    awsRegion?.let { withRegion(it) }
                }.build()!!
    }

    constructor(s3Client: AmazonS3) {
        this.s3Client = s3Client
    }


    @Suppress("UNCHECKED_CAST")
    override fun write(record: AuditRecordInternal) {
        val row = ClickhouseRecordSerializer.serialize(record)
        buffer.add(InsertExpression(AuditTable(), Row(row as Map<Column<Any, DbType<Any>>, Any>)).toSql())
    }

    override fun flush() = close()

    override fun close() {
        if (buffer.isNotEmpty()) {
            val key = "Failover_" + formatter.print(DateTime.now().withZone(DateTimeZone.UTC))
            s3Client.putObject(bucketName, key, buffer.joinToString(separator = "\n"))
            buffer.clear()
        }
    }
}