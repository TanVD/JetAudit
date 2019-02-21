package tanvd.jetaudit.implementation.writer

import org.slf4j.LoggerFactory
import tanvd.jetaudit.model.internal.AuditRecordInternal
import tanvd.jetaudit.utils.Conf
import tanvd.jetaudit.utils.PropertyLoader

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

        private val reserveWriterType: String by lazy { PropertyLoader[Conf.RESERVE_WRITER] }

        private val internalWriter: AuditReserveWriter by lazy {
            val reservePath = PropertyLoader[Conf.RESERVE_PATH]
            when (reserveWriterType) {
                "File" -> ClickhouseSqlFileWriter(reservePath)
                "Log" -> ClickhouseSqlLogWriter(reservePath)
                "S3" -> ClickhouseSqlS3Writer()
                else -> {
                    logger.error("Unknown option -- $reserveWriterType for reserve writing. Fallback to File.")
                    ClickhouseSqlFileWriter(reservePath)
                }
            }
        }

        fun getWriter(): AuditReserveWriter = internalWriter
    }
}
