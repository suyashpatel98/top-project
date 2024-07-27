Use object pool instead of managing connections yourself. Something like the following
```java
public class SchemaAwareConnectionPool {
    private static final Logger logger = LoggerFactory.getLogger(SchemaAwareConnectionPool.class);

    private final GenericObjectPool<PoolableConnection> connectionPool;

    public SchemaAwareConnectionPool(BasicDataSource dataSource, int maxConnections) {
        PoolableConnectionFactory factory = new PoolableConnectionFactory(dataSource.getConnectionFactory(), null);
        
        GenericObjectPoolConfig<PoolableConnection> config = new GenericObjectPoolConfig<>();
        config.setMaxTotal(maxConnections);
        config.setTestOnBorrow(true);
        config.setTestOnReturn(true);
        
        this.connectionPool = new GenericObjectPool<>(factory, config);
        factory.setPool(connectionPool);
    }

    public Connection getConnection(String schema) throws SQLException {
        try {
            PoolableConnection connection = connectionPool.borrowObject();
            if (!schema.equals(connection.getSchema())) {
                connection.setSchema(schema);
            }
            logger.info("Returning connection for schema: {}", schema);
            return connection;
        } catch (Exception e) {
            throw new SQLException("Error getting connection from pool", e);
        }
    }

    public void returnConnection(Connection connection) throws SQLException {
        if (connection instanceof PoolableConnection) {
            try {
                connectionPool.returnObject((PoolableConnection) connection);
            } catch (Exception e) {
                throw new SQLException("Error returning connection to pool", e);
            }
        } else {
            connection.close();
        }
    }

    public void close() {
        connectionPool.close();
    }
}
```
