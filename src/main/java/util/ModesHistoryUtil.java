package util;

import etl.DTO.GeoHistory;
import etl.DTO.ModesHistory;
import org.postgis.LineString;
import org.postgis.MultiLineString;
import org.postgis.PGgeometry;
import org.postgis.Point;
import org.springframework.jdbc.core.*;
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
}
