package tanvd.audit.serializers

import tanvd.audit.model.external.AuditSerializer

internal object LongSerializer : AuditSerializer<Long> {
    override fun deserialize(serializedString: String): Long {
        return serializedString.toLong()
    }

    override fun serialize(value: Long): String {
        return value.toString()
    }

    override fun display(value: Long): String {
        return value.toString()
    }
}
