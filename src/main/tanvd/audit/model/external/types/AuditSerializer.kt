package tanvd.audit.model.external.types

interface AuditSerializer<T> {
    /**
     * If it can not find entity null instead will be returned
     */
    fun deserializeBatch(serializedStrings: List<String>): Map<String, T?> {
        val deserializedMap: MutableMap<String, T?> = HashMap()
        for (string in serializedStrings) {
            deserializedMap.put(string, deserialize(string))
        }
        return deserializedMap
    }

    /**
     * If it can not find entity null instead will be returned
     */
    fun deserialize(serializedString: String): T?

    fun serialize(value: T): String

    fun display(value: T): String
}
