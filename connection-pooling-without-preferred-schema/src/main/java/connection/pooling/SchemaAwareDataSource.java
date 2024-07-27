package connection.pooling;

import org.apache.commons.dbcp2.BasicDataSource;
import org.springframework.jdbc.datasource.AbstractDataSource;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

public class SchemaAwareDataSource extends AbstractDataSource {

    private final BasicDataSource dataSource;
    private final ThreadLocal<String> schemaNameHolder = new ThreadLocal<>();

    public SchemaAwareDataSource(BasicDataSource dataSource) {
        this.dataSource = dataSource;
    }

    @Override
    public Connection getConnection() throws SQLException {
        Connection connection = dataSource.getConnection();
        String schemaName = schemaNameHolder.get();
        if (schemaName != null) {
            try (var statement = connection.createStatement()) {
                statement.execute("USE " + schemaName);
            }
        }
        return connection;
    }

    @Override
    public Connection getConnection(String username, String password) throws SQLException {
        throw new UnsupportedOperationException("Not supported");
    }

    public void setSchemaName(String schemaName) {
        schemaNameHolder.set(schemaName);
    }

    public void clearSchemaName() {
        schemaNameHolder.remove();
    }
}
