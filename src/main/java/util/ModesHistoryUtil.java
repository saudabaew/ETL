package util;

import etl.DTO.GeoHistory;
import etl.DTO.ModesHistory;
import etl.DTO.Sensor;
import org.postgis.LineString;
import org.postgis.MultiLineString;
import org.postgis.PGgeometry;
import org.postgis.Point;
import org.springframework.jdbc.core.*;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;
import org.springframework.jdbc.core.simple.SimpleJdbcCall;
import org.springframework.stereotype.Component;

import java.sql.*;
import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;

@Component
public class ModesHistoryUtil {

    //добавление объектов ModesHistory в БД
    public void insertBatchModesHistory(List<ModesHistory> modesHistoryList, JdbcTemplate jdbcTemplateAgrotronic) {
        long startInsertModes = System.currentTimeMillis();
        jdbcTemplateAgrotronic.batchUpdate("INSERT INTO modes_history(imei, time_start, time_end, line, mode_id) " +
                        "VALUES (?,?,?,?,?)",
                new BatchPreparedStatementSetter() {
                    @Override
                    public void setValues(PreparedStatement ps, int i) throws SQLException {
                        ModesHistory modesHistory = modesHistoryList.get(i);
                        ps.setLong(1, modesHistory.getImei());
                        ps.setTimestamp(2, Timestamp.valueOf(modesHistory.getTimeStart()));
                        ps.setTimestamp(3, Timestamp.valueOf(modesHistory.getTimeEnd()));
                        ps.setObject(4, new PGgeometry(modesHistory.getLine()));
                        ps.setLong(5, modesHistory.getMode());
                    }

                    @Override
                    public int getBatchSize() {
                        return modesHistoryList.size();
                    }

                });
        long finishInsertModes = System.currentTimeMillis();
        long insertTime = finishInsertModes - startInsertModes;
        System.out.println("Insert modes_history time = " + insertTime + "ms. Added " + modesHistoryList.size() + " records. Recording speed "
                + ((double) modesHistoryList.size() / insertTime) * 1000 + " modes/s");
    }

    //возвращает набор объектов ModesHistory
    public List<ModesHistory> getModesHistory(List<GeoHistory> geoHistoryList) {
        List<ModesHistory> modesHistoryList = new ArrayList<>();

        //уникальные imei для заданного набора geoHistory
        Set<Long> imeiSet = new HashSet<>();
        imeiSet = geoHistoryList.stream().map(GeoHistory::getImei).collect(Collectors.toSet());

        for (Long imei : imeiSet) {
            //набор GeoHistory для IMEI
            List<GeoHistory> geoHistoriesOfImei = new ArrayList<>();
            for (GeoHistory geoHistory : geoHistoryList) {
                if (geoHistory.getImei() == imei) geoHistoriesOfImei.add(geoHistory);
            }

            //временные отрезки для данного imei
            List<LocalDateTime> timeListOfImei = new ArrayList<>();
            for (GeoHistory geoHistory : geoHistoryList) {
                if (geoHistory.getImei() == imei) timeListOfImei.add(geoHistory.getDate());
            }

            //сортировка времени по возрастанию
            Collections.sort(timeListOfImei);

            //карта с указанием режима работы по времени
            Map<LocalDateTime, Long> modeMap = new HashMap<>();
            for (GeoHistory geoHistory : geoHistoriesOfImei) {
                modeMap.put(geoHistory.getDate(), geoHistory.getMode());
            }
            //карта с указанием LON по времени
            Map<LocalDateTime, Double> lonMap = new HashMap<>();
            for (GeoHistory geoHistory : geoHistoriesOfImei) {
                lonMap.put(geoHistory.getDate(), geoHistory.getLon());
            }
            //карта с указанием LON по времени
            Map<LocalDateTime, Double> latMap = new HashMap<>();
            for (GeoHistory geoHistory : geoHistoriesOfImei) {
                latMap.put(geoHistory.getDate(), geoHistory.getLat());
            }

            //начинаем с режима "Нет данных"
            long mode = 0;
            //все географические точки режима работы
            List<Point> pointList = new ArrayList<>();
            LocalDateTime timeStart = timeListOfImei.get(0);
            double lonStart = lonMap.get(timeStart);
            double latStart = latMap.get(timeStart);
            LocalDateTime timeEnd;
            //перебор показателей от определенного IMEI с учетом времени
            for (int i = 1; i < timeListOfImei.size(); i++) {
                //текущее время для точки
                LocalDateTime time = timeListOfImei.get(i);
                //строим географические координаты для точки
                double lon = lonMap.get(time);
                double lat = latMap.get(time);
                pointList.add(new Point(lon, lat));
                //текущий режим работы
                Long currentMode = modeMap.get(time);
                //если новый режим не совпадает с текущим
                if (mode != currentMode) {
                    try {
                        timeEnd = timeListOfImei.get(i + 1);
                    } catch (IndexOutOfBoundsException e) {
                        //если дошли до конца массива
                        timeEnd = timeListOfImei.get(timeListOfImei.size() - 1);
                    }
                    //дописываем точку старта
                    pointList.add(new Point(lonStart, latStart));
                    //формируем массив точек
                    Point[] points = pointList.toArray(new Point[pointList.size()]);
                    //строим линию
                    MultiLineString multiLineString = new MultiLineString(new LineString[]{
                            new LineString(points)
                    });
                    //задаем для линии SRID
                    multiLineString.setSrid(3857);
                    modesHistoryList.add(new ModesHistory(imei, timeStart, timeEnd, multiLineString, mode));
                    //устанавливаем новые текущие значения режима, времени и точки его начала
                    mode = currentMode;
                    timeStart = timeEnd;
                    lonStart = lon;
                    latStart = lat;
                    //очищаем массив предыдущих точек
                    pointList.clear();
                }
            }
        }
        return modesHistoryList;
    }

    public int modesHistoryOfStoredProcedure(long imei, LocalDateTime date, JdbcTemplate jdbcTemplateAgrotronic) {

        SimpleJdbcCall jdbcCall = new SimpleJdbcCall(jdbcTemplateAgrotronic)
                .withCatalogName("public")
                .withProcedureName("get_sensors_for_mode")
                .returningResultSet("tt", BeanPropertyRowMapper.newInstance(Sensor.class));

        SqlParameterSource in = new MapSqlParameterSource()
                .addValue("imei", imei)
                .addValue("d_from", date)
                .addValue("tt", "tt");

        Map<String, Object> out = jdbcCall.execute(in);

        for (Map.Entry entry : out.entrySet()) {
            System.out.println("Key: " + entry.getKey() + " Value: "
                    + entry.getValue());
        }

        List sensorList = (List) out.get("tt");
        System.out.println(sensorList.size());

        Sensor sensor = new Sensor();
        sensor.setParam_name((String) out.get("param_name"));
        sensor.setValue((String) out.get("value"));
        //System.out.println(sensor.toString());

        return 0;
    }

    public List<Sensor> sensorListForMode(long imei, LocalDateTime date, JdbcTemplate jdbcTemplateAgrotronic) {
        String sql = "WITH sens AS (\n" +
                "         SELECT id, name,\n" +
                "                               param AS param_name, \n" +
                "                               valid_period_seconds, \n" +
                "                               default_val,\n" +
                "                               id_unit,\n" +
                "                               unit_type\n" +
                "           FROM sensors\n" +
                "        ) \n" +
                "SELECT t2.param_name:: text, (Select CASE\n" +
                "                                                               WHEN t2.valid_period_seconds is not null\n" +
                "                                                               then\n" +
                "                               (WITH values as\n" +
                "                               (SELECT t1.value\n" +
                "                                                                 FROM public.sensor_history t1\n" +
                "                                                                 where t1.imei = " + imei +
                "                                                                                              and t1.date <= '" + date + "'\n" +
                "                                                                                              and t1.date >= (timestamp '" + date + "' - CASE \n" +
                "                                                                                                      WHEN( make_interval (secs => t2.valid_period_seconds)) is not null \n" +
                "                                                                                                      THEN ( make_interval (secs => t2.valid_period_seconds) )\n" +
                "                                                                                                      ELSE interval '0' end) \n" +
                "                                                                                              and t1.param_name = t2.param_name order by date desc limit 1)\n" +
                "                               Select CASE\n" +
                "                                                               WHEN\n" +
                "                                                               (SELECT tt1.value  FROM values tt1)is not null\n" +
                "                                                                              THEN  ((SELECT tt2.value  FROM values tt2) ::text)\n" +
                "                                                                              ELSE ((Select default_val from sens s where s.id = t2.id )::text)\n" +
                "                                                               END)\n" +
                "                                                               else \n" +
                "                               (SELECT t1.value\n" +
                "                                                                 FROM public.sensor_history t1\n" +
                "                                                                 where t1.imei = " + imei + " and t1.date <= '" + date + "' and t1.param_name = t2.param_name order by date desc limit 1)\n" +
                "                               end):: text  as value\n" +
                "\n" +
                "FROM sens t2 where t2.id_unit = (SELECT tt3.id  FROM public.units tt3 where tt3.imei = " + imei + ") or \n" +
                "                                                               t2.unit_type = (SELECT tt4.unit_type  FROM public.units tt4 where tt4.imei = " + imei + ") and t2.param_name is not null";
        List<Sensor> sensorList = new ArrayList<>();

        List<Map<String, Object>> rows = jdbcTemplateAgrotronic.queryForList(sql);
        for (Map row : rows) {
            Sensor sensor = new Sensor();
            sensor.setParam_name((String) row.get("param_name"));
            sensor.setValue((String) row.get("value"));
            sensorList.add(sensor);
        }
        return sensorList;
    }
}
