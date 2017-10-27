package tanvd.aorm.model

sealed class DbType {
    open fun toSqlName() : String {
        TODO("Not implemented")
    }
}

class DbDate : DbType() {
    override fun toSqlName(): String {
        return "Date"
    }
}

class DbDateTime: DbType() {
    override fun toSqlName(): String {
        return "DateTime"
    }
}

class DbLong: DbType() {
    override fun toSqlName(): String {
        return "Int64"
    }
}

class DbULong: DbType() {
    override fun toSqlName(): String {
        return "UInt64"
    }
}

class DbBoolean: DbType() {
    override fun toSqlName(): String {
        return "UInt8"
    }
}

class DbString: DbType() {
    override fun toSqlName(): String {
        return "String"
    }
}

class DbArrayDate: DbType() {
    override fun toSqlName(): String {
        return "Array(Date)"
    }
}

class DbArrayDateTime: DbType() {
    override fun toSqlName(): String {
        return "Array(DateTime)"
    }
}

class DbArrayLong: DbType() {
    override fun toSqlName(): String {
        return "Array(Int64)"
    }
}

class DbArrayULong: DbType() {
    override fun toSqlName(): String {
        return "Array(UInt64)"
    }
}

class DbArrayBoolean: DbType() {
    override fun toSqlName(): String {
        return "Array(UInt8)"
    }
}

class DbArrayString: DbType() {
    override fun toSqlName(): String {
        return "Array(String)"
    }
}

