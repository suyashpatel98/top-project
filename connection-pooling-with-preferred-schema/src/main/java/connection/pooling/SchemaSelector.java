package connection.pooling;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.sql.SQLException;

@Component
public class SchemaSelector {

    private final SchemaAwareConnectionPool connectionPool;

    @Autowired
    public SchemaSelector(SchemaAwareConnectionPool connectionPool) {
        this.connectionPool = connectionPool;
    }

    public SchemaAwareConnection getConnectionForSchema(String schemaName) throws SQLException {
        return connectionPool.getConnection(schemaName);
    }
}