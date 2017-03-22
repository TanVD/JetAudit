package tanvd.audit.implementation

import tanvd.audit.implementation.dao.AuditDao
import tanvd.audit.implementation.dao.AuditDaoFactory
import tanvd.audit.model.AuditRecord
import java.util.*
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit

/**
 * Saves audits in DB
 */
class AuditWorker(val auditQueue: LinkedBlockingQueue<AuditRecord>, val maxBuffer : Int = 5000,
                  val timeToWait: Long = 10) {

    val buffer: MutableList<AuditRecord> = ArrayList()

    val auditDao : AuditDao

    var isWorking : Boolean = true

    init {
        auditDao = AuditDaoFactory.getDao()
    }

    //TODO Check if reconnection too slow
    fun start() {
        while (true) {
            synchronized (isWorking) {
                val record = auditQueue.poll(timeToWait, TimeUnit.MILLISECONDS)
                isWorking = record != null || buffer.isNotEmpty()
                if (record == null && buffer.isNotEmpty()) {
                    auditDao.saveRecords(buffer.take(buffer.size))
                    buffer.clear()
                }
                if (record != null) {
                    if (buffer.size == maxBuffer) {
                        auditDao.saveRecords(buffer.take(buffer.size))
                        buffer.clear()
                    }
                    buffer.add(record as AuditRecord)
                }
            }

        }
    }
}