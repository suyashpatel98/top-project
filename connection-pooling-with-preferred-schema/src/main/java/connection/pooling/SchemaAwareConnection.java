package connection.pooling;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class SchemaAwareConnection implements AutoCloseable {
    private final Connection connection;
    private String currentSchema;

    public SchemaAwareConnection(Connection connection) {
        this.connection = connection;
    }

    public Connection getConnection() {
        return connection;
    }

    public String getCurrentSchema() {
        return currentSchema;
    }

    public void setSchema(String schema) throws SQLException {
        if (!schema.equals(currentSchema)) {
            try (Statement stmt = connection.createStatement()) {
                stmt.execute("USE " + schema);
                currentSchema = schema;
            }
        }
    }

    @Override
    public void close() throws SQLException {
        // Here we don't actually close the connection, just return it to the pool
        // The actual closing of connections should be handled by the pool itself
    }
}
