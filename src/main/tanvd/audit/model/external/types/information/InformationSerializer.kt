package tanvd.audit.model.external.types.information

interface InformationSerializer<T : Any> {
    /**
     * If it can not find entity null instead will be returned
     */
    fun deserialize(serialized: String): T

    fun serialize(entity: T): String
}