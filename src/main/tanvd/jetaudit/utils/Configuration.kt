package tanvd.jetaudit.utils

enum class Conf(val paramName: String, val defaultValue: String?) {

    QUEUE_CAPACITY("capacityOfQueue", "20000"),
    WORKERS_NUM("numberOfWorkers", "3"),
    RESERVE_WRITER("reserveWriter", "File"),
    RESERVE_PATH("reservePath", "reserve.txt"),
    AUDIT_TABLE("auditTable", "AuditTable"),
    AUDIT_DATABASE("auditDatabase", "default"),
    DEFAULT_DDL("useDefaultDDL", "true"),
    WORKER_BUFFER("capacityOfWorkerBuffer", "5000"),
    WAITING_QUEUE_TIME("waitingQueueTime", "10"),
    MAX_GENERATION("maxGeneration", "15"),
    S3_FAILOVER("S3BucketFailover", "ClickhouseFailover"),
    AWS_REGION("AWSRegion", null);

    companion object {
        const val NAMESPACE = "jetAudit"
    }

    fun propertyName() = "$NAMESPACE.$paramName"
}