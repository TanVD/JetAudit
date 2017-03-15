package tanvd.audit.implementation

import tanvd.audit.AuditRecord
import java.util.concurrent.LinkedBlockingQueue

/**
 * Saves audits in DB
 */
class AuditWorker(val auditQueue: LinkedBlockingQueue<AuditRecord>) {
    val auditDao : AuditDao

    init {
        auditDao = AuditDao()
    }

    fun start() {
        while (true) {
            val record = auditQueue.take()
            auditDao.saveRow(record)
        }
    }
}