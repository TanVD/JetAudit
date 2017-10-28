//package tanvd.audit.implementation.writer
//
//import org.slf4j.Logger
//import org.slf4j.LoggerFactory
//import tanvd.audit.implementation.clickhouse.AuditDaoClickhouseImpl
//import tanvd.audit.implementation.clickhouse.ClickhouseRecordSerializer
//import tanvd.audit.model.internal.AuditRecordInternal
//
//internal class ClickhouseSqlLogWriter : AuditReserveWriter {
//
//    private val writer: Logger
//
//    constructor(loggerName: String) {
//        writer = LoggerFactory.getLogger(loggerName)
//    }
//
//    constructor(writer: Logger) {
//        this.writer = writer
//    }
//
//    override fun flush() {
//        //nothing to do here
//    }
//
//    override fun write(record: AuditRecordInternal) {
//        val row = ClickhouseRecordSerializer.serialize(record)
//        val sqlInsert = "INSERT INTO ${AuditDaoClickhouseImpl.auditTable} (${row.toStringHeader()})" +
//                " VALUES (${row.toValues()});"
//        writer.error(sqlInsert)
//    }
//
//    override fun close() {
//        //nothing to do here
//    }
//
//
//}