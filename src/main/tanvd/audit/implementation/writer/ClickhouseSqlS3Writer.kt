package tanvd.audit.implementation.writer

import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import tanvd.audit.implementation.clickhouse.AuditDaoClickhouseImpl
import tanvd.audit.implementation.clickhouse.ClickhouseRecordSerializer
import tanvd.audit.model.internal.AuditRecordInternal
import tanvd.audit.utils.PropertyLoader
import tanvd.audit.utils.RandomGenerator

internal class ClickhouseSqlS3Writer : AuditReserveWriter {

    private val bucketName by lazy { PropertyLoader["S3BucketFailover"] ?: "ClickhouseFailover" }

    private val buffer = ArrayList<String>()

    val s3Client: AmazonS3

    constructor() {
        val builder = AmazonS3ClientBuilder.standard()!!
        builder.region = "eu-west-1"
        s3Client = builder.build()!!

//        s3Client = AmazonS3ClientBuilder.standard().withCredentials(InstanceProfileCredentialsProvider(true)).build()!!
    }

    constructor(s3Client: AmazonS3) {
        this.s3Client = s3Client
    }


    override fun write(record: AuditRecordInternal) {
        val row = ClickhouseRecordSerializer.serialize(record)
        buffer.add("INSERT INTO ${AuditDaoClickhouseImpl.auditTable} (${row.toStringHeader()})" +
                " VALUES (${row.toValues()});")
    }

    override fun flush() {
        if (buffer.isNotEmpty()) {
            val key = "Failover_" + RandomGenerator.next().toString()
            s3Client.putObject(bucketName, key, buffer.joinToString(separator = "\n"))
            buffer.clear()
        }
    }

    override fun close() {
        if (buffer.isNotEmpty()) {
            val key = "Failover_" + RandomGenerator.next().toString()
            s3Client.putObject(bucketName, key, buffer.joinToString(separator = "\n"))
            buffer.clear()
        }
    }
}