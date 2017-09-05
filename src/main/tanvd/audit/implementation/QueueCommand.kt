package tanvd.audit.implementation

import tanvd.audit.model.internal.AuditRecordInternal

internal interface QueueCommand

internal data class SaveRecords(val recordsInternal: List<AuditRecordInternal>) : QueueCommand {
    constructor(vararg recordsInternal: AuditRecordInternal) : this(recordsInternal.toList())
}

internal class ShutDown : QueueCommand