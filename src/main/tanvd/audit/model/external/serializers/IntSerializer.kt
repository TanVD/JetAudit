package tanvd.audit.model.external.serializers

import tanvd.audit.model.external.types.AuditSerializer

internal object IntSerializer : AuditSerializer<Int> {
    override fun deserialize(serializedString: String): Int {
        return serializedString.toInt()
    }

    override fun serialize(entity: Int): String {
        return entity.toString()
    }

    override fun display(entity: Int): String {
        return entity.toString()
    }
}