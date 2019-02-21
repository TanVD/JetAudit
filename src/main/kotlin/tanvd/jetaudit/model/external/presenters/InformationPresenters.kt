package tanvd.jetaudit.model.external.presenters

import tanvd.jetaudit.implementation.clickhouse.aorm.AuditTable
import tanvd.jetaudit.model.external.types.information.InformationType
import java.util.*

object TimeStampType : InformationType<Long>(AuditTable.timestamp)

object VersionType : InformationType<Long>(AuditTable.version)

object IdType : InformationType<Long>(AuditTable.id)

object DateType : InformationType<Date>(AuditTable.date)

object IsDeletedType : InformationType<Boolean>(AuditTable.isDeleted)