package org.babyfish.jimmer.sql.cache.transaction;

import org.babyfish.jimmer.sql.meta.DatabaseSchemaStrategy;
import org.babyfish.jimmer.sql.meta.DefaultDatabaseSchemaStrategy;

import java.sql.SQLException;
import java.sql.Statement;

public class H2TransactionCacheOperatorWithSchemaTest extends H2TransactionCacheOperatorTest {

    private static final String SCHEMA = "MYSCHEMA";

    @Override
    protected String databaseUrl() {
        return "jdbc:h2:mem:trans-cache-operator-schema";
    }

    @Override
    protected void assume() {
        // connection() keeps _con open — H2 in-memory database persists only while a connection is alive
        try (Statement stmt = connection().createStatement()) {
            stmt.execute("CREATE SCHEMA IF NOT EXISTS " + SCHEMA);
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    protected DatabaseSchemaStrategy databaseSchemaStrategy() {
        return new DefaultDatabaseSchemaStrategy(SCHEMA);
    }
}
