package org.egov.collection.repository.rowmapper;

import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.stereotype.Service;

import java.sql.ResultSet;
import java.sql.SQLException;

@Service
public class BillIdRowMapper implements ResultSetExtractor<String> {
    @Override
    public String extractData(ResultSet rs) throws SQLException, DataAccessException {
        String response = null;
        while(rs.next()) {
            response = rs.getString("billid");
        }
        return response;
    }
}
