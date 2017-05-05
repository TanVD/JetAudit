package tanvd.audit.model.external.serializers

import tanvd.audit.model.external.types.AuditSerializer

internal object StringSerializer : AuditSerializer<String> {
    override fun deserialize(serializedString: String): String {
        return serializedString
    }

    override fun serialize(entity: String): String {
        return entity
    }
}