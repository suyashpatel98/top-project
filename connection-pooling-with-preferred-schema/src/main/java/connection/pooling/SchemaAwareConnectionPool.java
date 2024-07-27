package connection.pooling;

import org.apache.commons.dbcp2.BasicDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class SchemaAwareConnectionPool {
    private static final Logger logger = LoggerFactory.getLogger(SchemaAwareConnectionPool.class);

    private final BasicDataSource dataSource;
    private final ConcurrentHashMap<String, List<SchemaAwareConnection>> schemaConnections = new ConcurrentHashMap<>();
    private final List<SchemaAwareConnection> allConnections = Collections.synchronizedList(new ArrayList<>());
    private final int maxConnections;

    public SchemaAwareConnectionPool(BasicDataSource dataSource, int maxConnections) {
        this.dataSource = dataSource;
        this.maxConnections = maxConnections;
    }

    public synchronized SchemaAwareConnection getConnection(String schema) throws SQLException {
        List<SchemaAwareConnection> connections = schemaConnections.computeIfAbsent(schema, k -> Collections.synchronizedList(new ArrayList<>()));

        // Check if there's an available connection for the preferred schema
        for (SchemaAwareConnection conn : connections) {
            if (!conn.getConnection().isClosed() && !conn.getConnection().isReadOnly()) {
                logger.info("Returning existing connection for schema: {}", schema);
                return conn;
            }
        }

        // If no connection available for the preferred schema, check for any idle connection
        for (SchemaAwareConnection conn : allConnections) {
            if (!conn.getConnection().isClosed()) {
                conn.setSchema(schema);
                connections.add(conn);
                logger.info("Reusing idle connection and setting schema to: {}", schema);
                return conn;
            }
        }

        // If no idle connections available and we haven't reached the max, create a new one
        if (allConnections.size() < maxConnections) {
            SchemaAwareConnection newConn = createNewConnection(schema);
            connections.add(newConn);
            allConnections.add(newConn);
            logger.info("Created new connection for schema: {}", schema);
            return newConn;
        }

        throw new SQLException("No available connections in the pool");
    }

    private SchemaAwareConnection createNewConnection(String schema) throws SQLException {
        SchemaAwareConnection conn = new SchemaAwareConnection(dataSource.getConnection());
        conn.setSchema(schema);
        return conn;
    }
}
