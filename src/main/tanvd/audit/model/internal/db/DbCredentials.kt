package tanvd.audit.model.internal.db

import tanvd.audit.utils.PropertyLoader

data class DbCredentials(val username: String, val password: String, val url: String) {
    constructor() : this(PropertyLoader["Username"]!!, PropertyLoader["Password"]!!, PropertyLoader["Url"]!!)
}