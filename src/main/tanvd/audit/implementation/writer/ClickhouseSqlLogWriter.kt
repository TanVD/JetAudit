package tanvd.audit.implementation.writer

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import tanvd.aorm.DbType
import tanvd.aorm.InsertExpression
import tanvd.aorm.Row
import tanvd.aorm.expression.Column
import tanvd.audit.implementation.clickhouse.ClickhouseRecordSerializer
import tanvd.audit.implementation.clickhouse.aorm.AuditTable
import tanvd.audit.model.internal.AuditRecordInternal

internal class ClickhouseSqlLogWriter : AuditReserveWriter {

    private val writer: Logger

    constructor(loggerName: String) {
        writer = LoggerFactory.getLogger(loggerName)
    }

    constructor(writer: Logger) {
        this.writer = writer
    }

    override fun flush() {
        //nothing to do here
    }

    @Suppress("UNCHECKED_CAST")
    override fun write(record: AuditRecordInternal) {
        val row = ClickhouseRecordSerializer.serialize(record)
        writer.error(InsertExpression(AuditTable(), Row(row as Map<Column<Any, DbType<Any>>, Any>)).toSql())
    }

    override fun close() {
        //nothing to do here
    }


}