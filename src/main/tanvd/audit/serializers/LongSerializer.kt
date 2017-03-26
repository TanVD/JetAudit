package tanvd.audit.serializers

import tanvd.audit.model.external.AuditType

internal object LongSerializer : AuditType.AuditSerializer<Long> {
    override fun deserialize(serializedString: String): Long {
        return serializedString.toLong()
    }

    override fun serialize(value: Long): String {
        return value.toString()
    }

}