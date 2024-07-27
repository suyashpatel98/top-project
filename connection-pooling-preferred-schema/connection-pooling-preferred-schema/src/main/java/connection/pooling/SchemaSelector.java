package connection.pooling;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;

@Component
public class SchemaSelector {

    private final SchemaAwareDataSource dataSource;

    @Autowired
    public SchemaSelector(DataSource dataSource) {
        this.dataSource = (SchemaAwareDataSource) dataSource;
    }

    public void useSchema(String schemaName) {
        dataSource.setSchemaName(schemaName);
    }

    public void clearSchema() {
        dataSource.clearSchemaName();
    }
}
