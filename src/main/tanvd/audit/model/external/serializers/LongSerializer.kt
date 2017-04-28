package tanvd.audit.model.external.serializers

import tanvd.audit.model.external.types.AuditSerializer

internal object LongSerializer : AuditSerializer<Long> {
    override fun deserialize(serializedString: String): Long {
        return serializedString.toLong()
    }

    override fun serialize(entity: Long): String {
        return entity.toString()
    }

    override fun display(entity: Long): String {
        return entity.toString()
    }
}
