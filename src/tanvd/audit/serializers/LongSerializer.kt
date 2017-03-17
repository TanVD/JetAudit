package tanvd.audit.serializers

import tanvd.audit.model.AuditSerializer

object LongSerializer : AuditSerializer<Long> {
    override fun deserialize(serializedString: String): Long {
        return serializedString.toLong()
    }

    override fun serialize(value: Long): String {
        return value.toString()
    }

}