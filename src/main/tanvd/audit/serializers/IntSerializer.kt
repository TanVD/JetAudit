package tanvd.audit.serializers

import tanvd.audit.model.external.AuditSerializer

internal object IntSerializer : AuditSerializer<Int> {
    override fun deserialize(serializedString: String): Int {
        return serializedString.toInt()
    }

    override fun serialize(value: Int): String {
        return value.toString()
    }

    override fun display(value: Int): String {
        return value.toString()
    }
}