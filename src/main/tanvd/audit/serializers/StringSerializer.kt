package tanvd.audit.serializers

import tanvd.audit.model.external.AuditType

internal object StringSerializer : AuditType.AuditSerializer<String> {
    override fun deserialize(serializedString: String): String {
        return serializedString
    }

    override fun serialize(value: String): String {
        return value
    }
}