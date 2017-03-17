package tanvd.audit.implementation

import tanvd.audit.implementation.dao.AuditDao
import tanvd.audit.implementation.dao.AuditDaoFactory
import tanvd.audit.model.AuditRecord
import java.util.concurrent.LinkedBlockingQueue

/**
 * Saves audits in DB
 */
class AuditWorker(val auditQueue: LinkedBlockingQueue<AuditRecord>) {
    val auditDao : AuditDao

    init {
        auditDao = AuditDaoFactory.getDao()
    }

    fun start() {
        while (true) {
            val record = auditQueue.take()
            auditDao.saveRow(record)
        }
    }
}