package tanvd.audit.implementation.writer

import org.slf4j.LoggerFactory
import tanvd.audit.model.internal.AuditRecordInternal
import tanvd.audit.utils.PropertyLoader

/**
 * Interface for reserve saving of exceptional audits.
 *
 * Beware, that normal work of reserve writing is essential condition to start program.
 */
internal interface AuditReserveWriter {

    fun write(record: AuditRecordInternal)

    fun flush()

    fun close()

    companion object AuditReserveWriterFactory {

        private val logger = LoggerFactory.getLogger(AuditReserveWriterFactory::class.java)

        private val reserveWriterType: String = PropertyLoader.loadProperty("ReserveWriter") ?: "File"

        private var internalWriter: AuditReserveWriter

        init {
            if (reserveWriterType == "File") {
                val reservePath = PropertyLoader.loadProperty("ReservePath") ?: "reserve.txt"
                internalWriter = ClickhouseSqlFileWriter(reservePath)
            } else if (reserveWriterType == "Log") {
                val reservePath = PropertyLoader.loadProperty("ReservePath") ?: "ReserveLogger"
                internalWriter = ClickhouseSqlLogWriter(reservePath)
            } else if (reserveWriterType == "S3") {
                internalWriter = ClickhouseSqlS3Writer()
            } else {
                logger.error("Unknown option for reserve writing. Fallback to File.")
                val reservePath = PropertyLoader.loadProperty("ReservePath") ?: "reserve.txt"
                internalWriter = ClickhouseSqlFileWriter(reservePath)
            }
        }

        fun getWriter(): AuditReserveWriter {
            return internalWriter
        }
    }
}
