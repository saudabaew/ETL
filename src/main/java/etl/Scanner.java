package etl;

import etl.DTO.*;
import org.postgis.LineString;
import org.postgis.MultiLineString;
import org.postgis.PGgeometry;
import org.postgis.Point;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.*;
import org.springframework.stereotype.Component;
import util.*;

import javax.annotation.PostConstruct;
import java.sql.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;

import static java.lang.Math.toIntExact;

@Component
public class Scanner {
    @Autowired
    //класс для соединения с БД
    private DataSourceUtil dataSourceUtil;
    @Autowired
    //класс для работы с сообщениями
    private MessageUtil messageUtil;
    @Autowired
    //класс для работы с параметрами
    private SensorUtil sensorUtil;
    @Autowired
    //класс для работы с параметрами
    private GeoHistoryUtil geoHistoryUtil;
    @Autowired
    //класс для работы с параметрами
    private ModesHistoryUtil modesHistoryUtil;

    private JdbcTemplate jdbcTemplateSakura, jdbcTemplateAgrotronic;
    private final Logger LOG = LoggerFactory.getLogger(Scanner.class);
    //начальный ID сообщения
    private long startId;
    //начальный ID из таблицы geo-history
    private long endIdForModesHistory;
    //максимальный ID сообщения, до которого делаем выборку
    private long maxId;
    //устанавливаем лимит порции сообщений
    public long limit = 250000;
    //набор машин с указанием их типа <IMEI, UNIT_TYPE>
    Map<Long, Long> mapUnits;

    public Scanner() {
    }

    //метод запускает алгоритм наполнения БД Agrotronic значениями из БД Sakura
    //@PostConstruct
    public void init() throws SQLException {
        jdbcTemplateSakura = new JdbcTemplate(dataSourceUtil.getDataSourceSakura());
        jdbcTemplateAgrotronic = new JdbcTemplate(dataSourceUtil.getDataSourceAgrotronic());
        List<Messages> messagesList;

        //получаем существующий набор машин с указанием их типа <IMEI, UNIT_TYPE>
        mapUnits = jdbcTemplateSakura.query("SELECT imei, unit_type FROM public.units",
                (ResultSet rs) -> {
                    HashMap<Long, Long> results = new HashMap<>();
                    while (rs.next()) {
                        results.put(rs.getLong("imei"), rs.getLong("unit_type"));
                    }
                    return results;
                });

        //получаем макисмальный ID сообщения в БД Sakura
        maxId = jdbcTemplateSakura.queryForObject("SELECT max(id) FROM messages", Long.class);

        //проверяем последний записанный ID в БД Agrotronic
        try {
            startId = jdbcTemplateAgrotronic.queryForObject("SELECT id_old FROM geo_history ORDER BY id DESC LIMIT 1", Long.class);
            ;
        } catch (Exception e) {
            //если ничего не записано, то начинаем с ID = 1
            startId = 150000000;
        }

        //записываем в таблицы DB Agrotronic пока не дойдем до максимального ID
        while (startId < maxId) {
            //получаем блок сообщения со startId по limit + startId
            messagesList = messageUtil.getMessages(startId, limit + startId, jdbcTemplateSakura);

            //получаем набор объектов для записи в таблицу geo_history
            List<GeoHistory> geoHistoryList = geoHistoryUtil.getGeoHistoryFromMessages(messagesList, mapUnits);
            //записываем объекты в таблицу geo_history
            geoHistoryUtil.insertBatchGeoHistory(geoHistoryList, jdbcTemplateAgrotronic);

            //получем набор объектов для записи в таблицу sensor_history
            List<Sensor> sensorList = sensorUtil.getSensors(messagesList);
            //записываем объекты в таблицу sensor_history
            sensorUtil.insertBatchSensor(sensorList, jdbcTemplateAgrotronic);

            //получем набор объектов из новой таблицы geo_history для записи в таблицу modes_history
            endIdForModesHistory = jdbcTemplateAgrotronic.queryForObject("SELECT id FROM geo_history ORDER BY id DESC LIMIT 1", Long.class);
            List<GeoHistory> geoHistoryListFromDB = geoHistoryUtil.getGeoHistoryFromDB(endIdForModesHistory-limit+1, endIdForModesHistory, jdbcTemplateAgrotronic);
            List<ModesHistory> modesHistoryList = modesHistoryUtil.getModesHistory(geoHistoryListFromDB);
            //записываем объекты в таблицу modes_history
            modesHistoryUtil.insertBatchModesHistory(modesHistoryList, jdbcTemplateAgrotronic);

            //запрашиваем новый стартовый ID
            startId = jdbcTemplateAgrotronic.queryForObject("SELECT id_old FROM geo_history ORDER BY id DESC LIMIT 1", Long.class) + 1;
            LOG.info("Inserted!____________________New start ID = " + startId);
        }

        //запись последней порции данных в DB Agrotronic
        if (startId - maxId < limit) {
            messagesList = messageUtil.getMessages(startId, maxId - startId, jdbcTemplateSakura);
            //получаем набор объектов для записи в таблицу geo_history
            List<GeoHistory> geoHistoryList = geoHistoryUtil.getGeoHistoryFromMessages(messagesList, mapUnits);
            //записываем объекты в таблицу geo_history
            geoHistoryUtil.insertBatchGeoHistory(geoHistoryList, jdbcTemplateAgrotronic);
            //получем набор объектов для записи в таблицу sensor_history
            List<Sensor> sensorList = sensorUtil.getSensors(messagesList);
            //записываем объекты в таблицу sensor_history
            sensorUtil.insertBatchSensor(sensorList, jdbcTemplateAgrotronic);
            //получем набор объектов для записи в таблицу modes_history
            List<ModesHistory> modesHistoryList = modesHistoryUtil.getModesHistory(geoHistoryList);
            //записываем объекты в таблицу modes_history
            modesHistoryUtil.insertBatchModesHistory(modesHistoryList, jdbcTemplateAgrotronic);
            LOG.info("_______Insert complete!_______");
        }
    }

    public void putDataForUnitType3Of5(Map<String, Double> testP11, Map<String, Double> testP12,
                                       Map<String, Double> testP45, Map<String, Double> testB22,
                                       Map<String, Double> testP15, Map<String, Double> testP50){
        String m1 = "M1";
        String m2 = "M2";
        String m3 = "M3";
        String m4 = "M4";
        String m5 = "M5";
        testP11.clear();
        testP12.clear();
        testP45.clear();
        testB22.clear();
        testP15.clear();
        testP50.clear();
        //тестовые данные для типов unitType3Of5
        testP11.put(m1, 10.0);
        testP12.put(m1, 400.0);
        testP50.put(m1, 20.0);
        testB22.put(m1, 0.0);

        testP11.put(m2, 0.0);
        testP12.put(m2, 400.0);
        testP50.put(m2, 0.0);
        testB22.put(m2, 0.0);

        testP11.put(m3, 0.4);
        testP12.put(m3, 600.0);
        testB22.put(m3, 0.0);

        testP11.put(m4, 10.0);
        testP12.put(m4, 600.0);
        testP50.put(m4, 40.0);
        testB22.put(m4, 1.0);

        testP11.put(m5, 0.3);
        testP12.put(m5, 600.0);
        testP50.put(m5, 40.0);
        testB22.put(m5, 1.0);
    }

    public void putDataForUnitType6Of16(Map<String, Double> testP11, Map<String, Double> testP12,
                                        Map<String, Double> testP45, Map<String, Double> testB22,
                                        Map<String, Double> testP15, Map<String, Double> testP50){
        String m1 = "M1";
        String m2 = "M2";
        String m3 = "M3";
        String m4 = "M4";
        String m5 = "M5";
        testP11.clear();
        testP12.clear();
        testP45.clear();
        testB22.clear();
        testP15.clear();
        testP50.clear();

        testP11.put(m1, 10.0);
        testP12.put(m1, 400.0);
        testP45.put(m1, 20.0);
        testB22.put(m1, 0.0);

        testP11.put(m2, 0.0);
        testP12.put(m2, 400.0);
        testP45.put(m2, 0.0);
        testB22.put(m2, 0.0);

        testP11.put(m3, 0.4);
        testP12.put(m3, 600.0);
        testB22.put(m3, 0.0);

        testP11.put(m4, 10.0);
        testP12.put(m4, 600.0);
        testP45.put(m4, 40.0);
        testB22.put(m4, 1.0);

        testP11.put(m5, 0.3);
        testP12.put(m5, 600.0);
        testP45.put(m5, 40.0);
        testB22.put(m5, 1.0);
    }

    public void putDataForUnitType17Of25(Map<String, Double> testP11, Map<String, Double> testP12,
                                         Map<String, Double> testP45, Map<String, Double> testB22,
                                         Map<String, Double> testP15, Map<String, Double> testP50){
        String m1 = "M1";
        String m2 = "M2";
        String m3 = "M3";
        String m4 = "M4";
        String m5 = "M5";
        testP11.clear();
        testP12.clear();
        testP45.clear();
        testB22.clear();
        testP15.clear();
        testP50.clear();

        testP11.put(m1, 10.0);
        testP12.put(m1, 400.0);
        testP15.put(m1, 20.0);
        testB22.put(m1, 0.0);

        testP11.put(m2, 0.0);
        testP12.put(m2, 400.0);
        testP15.put(m2, 0.0);
        testB22.put(m2, 0.0);

        testP11.put(m3, 0.4);
        testP12.put(m3, 600.0);
        testB22.put(m3, 0.0);

        testP11.put(m4, 10.0);
        testP12.put(m4, 600.0);
        testP15.put(m4, 40.0);
        testB22.put(m4, 1.0);

        testP11.put(m5, 0.3);
        testP12.put(m5, 600.0);
        testP15.put(m5, 40.0);
        testB22.put(m5, 1.0);
    }

    //@PostConstruct
    public void testOfMode() {
        //данные для тестирования режимов Mode в таблице geo_history
        List<Integer> unitType3Of5 = Arrays.asList(3, 4, 5);
        List<Integer> unitType6Of16 = Arrays.asList(6, 8, 9, 10, 11, 12, 13, 14, 15, 16);
        List<Integer> unitType17Of25 = Arrays.asList(17, 18, 19, 20, 21, 25);

        String m1 = "M1";
        String m2 = "M2";
        String m3 = "M3";
        String m4 = "M4";
        String m5 = "M5";

        Map<String, Double> testP11 = new HashMap<>();
        Map<String, Double> testP12 = new HashMap<>();
        Map<String, Double> testP45 = new HashMap<>();
        Map<String, Double> testB22 = new HashMap<>();
        Map<String, Double> testP15 = new HashMap<>();
        Map<String, Double> testP50 = new HashMap<>();

        //задайте тип техники
        int currentType = 6;
        //наполняем тестовыми данными в зависимости от типа техники
        if (currentType == 3) putDataForUnitType3Of5(testP11, testP12, testP45, testB22, testP15, testP50);

        if (currentType == 6) putDataForUnitType6Of16(testP11, testP12, testP45, testB22, testP15, testP50);

        if (currentType == 17) putDataForUnitType17Of25(testP11, testP12, testP45, testB22, testP15, testP50);

        long mode1 = sensorUtil.getMode(currentType, m1, testP11, testP12, testP45, testB22, testP15, testP50);
        System.out.println("Current type = " + currentType + ". Mode 1 = " + mode1);
        long mode2 = sensorUtil.getMode(currentType, m2, testP11, testP12, testP45, testB22, testP15, testP50);
        System.out.println("Current type = " + currentType + ". Mode 2 = " + mode2);
        long mode3 = sensorUtil.getMode(currentType, m3, testP11, testP12, testP45, testB22, testP15, testP50);
        System.out.println("Current type = " + currentType + ". Mode 3 = " + mode3);
        long mode4 = sensorUtil.getMode(currentType, m4, testP11, testP12, testP45, testB22, testP15, testP50);
        System.out.println("Current type = " + currentType + ". Mode 4 = " + mode4);
        long mode5 = sensorUtil.getMode(currentType, m5, testP11, testP12, testP45, testB22, testP15, testP50);
        System.out.println("Current type = " + currentType + ". Mode 5 = " + mode5);

    }

    @PostConstruct
    public void testOfSensor(){
        jdbcTemplateAgrotronic = new JdbcTemplate(dataSourceUtil.getDataSourceAgrotronic());

        String date = "2017-08-15 16:26:46";
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        LocalDateTime dateTime = LocalDateTime.parse(date, formatter);
        long imei = 864287037501648L;
        List<Sensor> sensorList = modesHistoryUtil.sensorListForMode(imei, dateTime, jdbcTemplateAgrotronic);

        //получаем существующий набор машин с указанием их типа <IMEI, UNIT_TYPE>
        mapUnits = jdbcTemplateAgrotronic.query("SELECT imei, unit_type FROM public.units",
                (ResultSet rs) -> {
                    HashMap<Long, Long> results = new HashMap<>();
                    while (rs.next()) {
                        results.put(rs.getLong("imei"), rs.getLong("unit_type"));
                    }
                    return results;
                });

        System.out.println("Mode = " + sensorUtil.modeOfSensor(mapUnits.get(imei), sensorList));
    }

    //функция заполнения таблицы modes_working
    public void putModesWorking() {

        List<Integer> integerList = jdbcTemplateSakura.query("SELECT id FROM public.unit_types",
                new RowMapper<Integer>() {
                    @Override
                    public Integer mapRow(ResultSet rs, int rowNum) throws SQLException {
                        return rs.getInt("id");
                    }
                });

        for (Integer id : integerList) {
            List<Mode> modeList = new ArrayList<>();
            Mode modeM0 = new Mode("M0", "FFFFE0", id, "Нет данных");
            Mode modeM1 = new Mode("M1", "D8BFD8", id, "Передвижение без убо");
            Mode modeM2 = new Mode("M2", "BC8F8F", id, "Выключен");
            Mode modeM3 = new Mode("M3", "FF0000", id, "Простой с включенным");
            Mode modeM4 = new Mode("M4", "008000", id, "Уборка/Работа");
            Mode modeM5 = new Mode("M5", "000080", id, "Уборка/Работа без пе");
            modeList.addAll(Arrays.asList(modeM0, modeM1, modeM2, modeM3, modeM4, modeM5));

            BatchPreparedStatementSetter statementSetter = new BatchPreparedStatementSetter() {
                @Override
                public void setValues(PreparedStatement ps, int i) throws SQLException {
                    Mode mode = modeList.get(i);
                    ps.setString(1, mode.getName());
                    ps.setString(2, mode.getColor());
                    ps.setLong(3, mode.getId_model());
                    ps.setString(4, mode.getComment());
                }

                @Override
                public int getBatchSize() {
                    return modeList.size();
                }
            };

            jdbcTemplateAgrotronic.batchUpdate("INSERT INTO modes_working(name, color, id_model, comment) VALUES (?, ?, ?, ?)", statementSetter);
        }
    }
}
