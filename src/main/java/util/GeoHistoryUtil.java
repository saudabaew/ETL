package util;

import etl.DTO.GeoHistory;
import etl.DTO.Messages;
import etl.DTO.Sensor;
import org.postgis.PGgeometry;
import org.postgis.Point;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Component;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.lang.Math.toIntExact;

@Component
public class GeoHistoryUtil {

    @Autowired
    //класс для работы с параметрами
    private SensorUtil sensorUtil;

    //добавление объектов GeoHistory в БД
    public void insertBatchGeoHistory(List<GeoHistory> geoHistoryList, JdbcTemplate jdbcTemplateAgrotronic) {
        long startInsertGeo = System.currentTimeMillis();
        jdbcTemplateAgrotronic.batchUpdate("INSERT INTO geo_history(id_old, imei, date, point, speed_gps, speed_sens, course, mode) " +
                        "VALUES (?,?,?, st_transform(st_setsrid(st_point(?,?),4326), 3857), ?,?,?,?)",
                new BatchPreparedStatementSetter() {
                    @Override
                    public void setValues(PreparedStatement ps, int i) throws SQLException {
                        GeoHistory geoHistory = geoHistoryList.get(i);
                        ps.setLong(1, geoHistory.getIdOld());
                        ps.setLong(2, geoHistory.getImei());
                        ps.setTimestamp(3, Timestamp.valueOf(geoHistory.getDate()));
                        ps.setDouble(4, geoHistory.getLon());
                        ps.setDouble(5, geoHistory.getLat());
                        ps.setDouble(6, geoHistory.getSpeedGps());
                        ps.setDouble(7, geoHistory.getSpeedSens());
                        ps.setInt(8, geoHistory.getCourse());
                        ps.setLong(9, geoHistory.getMode());
                    }

                    @Override
                    public int getBatchSize() {
                        return geoHistoryList.size();
                    }

                });
        long finishInsertGeo = System.currentTimeMillis();
        long insertTime = finishInsertGeo - startInsertGeo;
        System.out.println("Insert geo_history time = " + insertTime + "ms. Added " + geoHistoryList.size() + " records. Recording speed "
                + ((double) geoHistoryList.size() / insertTime) * 1000 + " geo/s");
    }

    //возвращает набор объектов GeoHistory из пакета сообщений
    public List<GeoHistory> getGeoHistoryFromMessages(final List<Messages> messages, Map<Long, Long> mapUnits) {
        //значения параметров для конкретной машины в определенный момент времени <IMEI+DATE, значение параметра>
        Map<String, Double> parameterP11 = new HashMap<>(); //скорость движения
        Map<String, Double> parameterP12 = new HashMap<>(); //частота вращения коленвала двигателя
        Map<String, Double> parameterP45 = new HashMap<>(); //частота вращения молотильного барабана
        Map<String, Double> parameterB22 = new HashMap<>(); //привод НК включен
        Map<String, Double> parameterP15 = new HashMap<>(); //частота вращения ротора
        Map<String, Double> parameterP50 = new HashMap<>(); //частота вращения измельчающего барабана

        //набор для таблицы geo_history
        List<GeoHistory> geoHistoryList = new ArrayList<>();

        //получение параметров сообщения для последующего определения режима работы
        for (Messages message : messages) {
            String imeiAndDate = new StringBuilder()
                    .append(message.getImei())
                    .append(message.getTimestamp_create())
                    .toString();

            //парсим параметры сообщения
            List<Sensor> sensors = sensorUtil.sensorsParser(message);

            //если присутствует необходимый параметр, то сохраняем его с ключом imeiAndDate
            for (Sensor sensor : sensors
                    ) {
                if (sensor.getParam_name().equals("P11"))
                    parameterP11.put(imeiAndDate, Double.parseDouble(sensor.getValue()));
                if (sensor.getParam_name().equals("P12"))
                    parameterP12.put(imeiAndDate, Double.parseDouble(sensor.getValue()));
                if (sensor.getParam_name().equals("P45"))
                    parameterP45.put(imeiAndDate, Double.parseDouble(sensor.getValue()));
                if (sensor.getParam_name().equals("B22"))
                    parameterB22.put(imeiAndDate, Double.parseDouble(sensor.getValue()));
                if (sensor.getParam_name().equals("P15"))
                    parameterP15.put(imeiAndDate, Double.parseDouble(sensor.getValue()));
                if (sensor.getParam_name().equals("P50"))
                    parameterP50.put(imeiAndDate, Double.parseDouble(sensor.getValue()));
            }
        }

        //получем набор объектов geo_history
        for (Messages message : messages) {
            long modeMessage;
            try {
                modeMessage = sensorUtil.getMode(toIntExact(mapUnits.get(message.getImei())), new StringBuilder()
                        .append(message.getImei())
                        .append(message.getTimestamp_create())
                        .toString(), parameterP11, parameterP12, parameterP45, parameterB22, parameterP15, parameterP50);
            } catch (Exception e) {
                modeMessage = 0;
            }
            long idOld = message.getId();
            long imei = message.getImei();
            LocalDateTime date = message.getTimestamp_create();
            Double lat = message.getLat();
            Double lon = message.getLon();
            double speedGps = message.getSpeed();
            double speedSens = 0;
            try {
                speedSens = parameterP11.get(new StringBuilder()
                        .append(imei)
                        .append(date)
                        .toString());
            } catch (Exception e) {
                speedSens = -1;
            }
            int course = message.getCourse();
            long mode = modeMessage;
            GeoHistory geoHistory = new GeoHistory(idOld, imei, date, lon, lat, speedGps, speedSens, course, mode);
            geoHistoryList.add(geoHistory);
        }

        return geoHistoryList;
    }

    public List<GeoHistory> getGeoHistoryFromDB(long startId, long limit, JdbcTemplate jdbcTemplateAgrotronic){
        List<GeoHistory> geoHistoryList = jdbcTemplateAgrotronic.query(
                "SELECT id_old, imei, date, point, speed_gps, speed_sens, course, mode\n" +
                        "              FROM public.geo_history WHERE id BETWEEN ? AND ? ORDER BY id ASC",
                new RowMapper<GeoHistory>() {
                    public GeoHistory mapRow(ResultSet rs, int rowNum) throws SQLException {
                        GeoHistory geoHistory = new GeoHistory();
                        geoHistory.setIdOld(rs.getInt("id_old"));
                        geoHistory.setImei(rs.getLong("imei"));
                        geoHistory.setDate(rs.getTimestamp("date").toLocalDateTime());
                        PGgeometry geom = (PGgeometry) rs.getObject("point");
                        Point point = (Point) geom.getGeometry();
                        geoHistory.setLon(point.getX());
                        geoHistory.setLat(point.getY());
                        geoHistory.setSpeedGps(rs.getDouble("speed_gps"));
                        geoHistory.setSpeedSens(rs.getDouble("speed_sens"));
                        geoHistory.setCourse(rs.getInt("course"));
                        geoHistory.setMode(rs.getInt("mode"));
                        return geoHistory;
                    }
                }, startId, limit);
        return geoHistoryList;
    }
}
