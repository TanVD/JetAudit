package tanvd.audit.serializers

import tanvd.audit.model.external.AuditSerializer

internal object StringSerializer : AuditSerializer<String> {
    override fun deserialize(serializedString: String): String {
        return serializedString
    }

    override fun serialize(value: String): String {
        return value
    }

    override fun display(value: String): String {
        return value;
    }
}