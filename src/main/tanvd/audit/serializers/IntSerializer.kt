package tanvd.audit.serializers

import tanvd.audit.model.external.AuditType

internal object IntSerializer : AuditType.AuditSerializer<Int> {
    override fun deserialize(serializedString: String): Int {
        return serializedString.toInt()
    }

    override fun serialize(value: Int): String {
        return value.toString()
    }

}