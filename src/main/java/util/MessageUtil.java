package util;

import etl.DTO.Messages;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Component;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

@Component
public class MessageUtil {
    //возвращает набор сообщений из БД со стартового ID в размере LIMIT
    public List<Messages> getMessages(long startId, long limit, JdbcTemplate jdbcTemplateSakura) {
        List<Messages> messages = jdbcTemplateSakura.query(
                "WITH hist AS (SELECT  id, timestamp_create, lat, lon, params, speed, imei,course\n" +
                        "              FROM public.messages WHERE id BETWEEN ? AND ? AND lat IS NOT NULL AND lon IS NOT NULL AND imei <> 0 ORDER BY id ASC)\n" +
                        "\n" +
                        "SELECT DISTINCT ON (timestamp_create, lat, lon,imei, params) id, timestamp_create, lat, lon, params, speed, imei,course\n" +
                        "FROM hist;",
                new RowMapper<Messages>() {
                    public Messages mapRow(ResultSet rs, int rowNum) throws SQLException {
                        Messages message = new Messages();
                        message.setId(rs.getLong("id"));
                        message.setTimestamp_create(rs.getTimestamp("timestamp_create").toLocalDateTime());
                        message.setLat(rs.getDouble("lat"));
                        message.setLon(rs.getDouble("lon"));
                        message.setParams(rs.getString("params"));
                        message.setSpeed(rs.getDouble("speed"));
                        message.setImei(rs.getLong("imei"));
                        message.setCourse(rs.getInt("course"));
                        return message;
                    }
                }, startId, limit);
        return messages;
    }
}
