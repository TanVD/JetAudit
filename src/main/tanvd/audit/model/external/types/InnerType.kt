package tanvd.audit.model.external.types

enum class InnerType {
    Long,
    //Please, do not use ULong to save data. This type is only for service needs and do not support queries.
    ULong,
    String,
    Boolean,
    Date,
    DateTime
}
