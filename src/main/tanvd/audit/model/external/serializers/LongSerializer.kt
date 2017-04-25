package tanvd.audit.model.external.serializers

import tanvd.audit.model.external.types.AuditSerializer

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
