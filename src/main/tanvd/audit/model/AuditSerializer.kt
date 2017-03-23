package tanvd.audit.model

interface AuditSerializer<T> {
    fun deserialize(serializedString : String) : T
    fun serialize(value : T) : String
}