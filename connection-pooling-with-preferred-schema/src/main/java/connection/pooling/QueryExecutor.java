package connection.pooling;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class QueryExecutor {

    private final SchemaSelector schemaSelector;

    @Autowired
    public QueryExecutor(SchemaSelector schemaSelector) {
        this.schemaSelector = schemaSelector;
    }

    public List<Map<String, Object>> executeQueryInSchema(String schemaName, String sql) throws SQLException {
        try (SchemaAwareConnection schemaConn = schemaSelector.getConnectionForSchema(schemaName);
             Connection conn = schemaConn.getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {

            List<Map<String, Object>> results = new ArrayList<>();
            while (rs.next()) {
                Map<String, Object> row = new HashMap<>();
                for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++) {
                    row.put(rs.getMetaData().getColumnName(i), rs.getObject(i));
                }
                results.add(row);
            }
            return results;
        }
    }
}
