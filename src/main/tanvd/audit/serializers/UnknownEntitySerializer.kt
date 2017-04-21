package tanvd.audit.serializers

import tanvd.audit.model.external.AuditSerializer
import tanvd.audit.model.external.AuditType
import tanvd.audit.model.external.UnknownEntity

internal object UnknownEntitySerializer : AuditSerializer<UnknownEntity> {
    override fun deserialize(serializedString: String): UnknownEntity {
        return UnknownEntity(serializedString, AuditType.resolveType(UnknownEntity::class))
    }

    override fun serialize(value: UnknownEntity): String {
        return "UnknownEntity"
    }

    override fun display(value: UnknownEntity): String {
        return "Unknown Entity with id = ${value.id} and type code = ${value.type.code}"
    }
}
