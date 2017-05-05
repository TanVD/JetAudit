package tanvd.audit.implementation.exceptions

internal class BasicDbException : Throwable {

    constructor(msg: String = "") : super(msg)

    constructor(msg: String, cause: Throwable) : super(msg, cause)
}
