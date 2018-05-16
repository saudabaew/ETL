package util;

import etl.DTO.Messages;
import etl.DTO.Sensor;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.lang.Math.toIntExact;

@Component
public class SensorUtil {

    //на основе типа и набора параметров определяем режим работы машины в конкретный момент времени
    public long getMode(int unitType, String imeiAndDate,
                        Map<String, Double> parameterP11,
                        Map<String, Double> parameterP12,
                        Map<String, Double> parameterP45,
                        Map<String, Double> parameterB22,
                        Map<String, Double> parameterP15,
                        Map<String, Double> parameterP50) {

        //виды режимов
        long m0 = 0;
        long m1 = 1;
        long m2 = 2;
        long m3 = 3;
        long m4 = 4;
        long m5 = 5;
        long mode = 0;

        switch (unitType) {
            //расчет режимов для КУК
            case 3:
            case 4:
            case 5:
                if (parameterP12.get(imeiAndDate) < 500 &&
                        parameterP11.get(imeiAndDate) < 0.5 &&
                        parameterP50.get(imeiAndDate) < 30 &&
                        parameterB22.get(imeiAndDate) != 1) {
                    mode = m1;
                    break;
                }
                if (parameterP12.get(imeiAndDate) <= 500 &&
                        parameterP11.get(imeiAndDate) <= 0.5) {
                    mode = m2;
                    break;
                }
                if (parameterP12.get(imeiAndDate) > 500 &&
                        parameterP11.get(imeiAndDate) <= 0.5 &&
                        parameterB22.get(imeiAndDate) != 1) {
                    mode = m3;
                    break;
                }
                if (parameterP12.get(imeiAndDate) > 500 &&
                        parameterP11.get(imeiAndDate) > 0.5 &&
                        parameterP50.get(imeiAndDate) > 30 &&
                        parameterB22.get(imeiAndDate) == 1) {
                    mode = m4;
                    break;
                }
                if (parameterP12.get(imeiAndDate) > 500 &&
                        parameterP11.get(imeiAndDate) <= 0.5 &&
                        parameterP50.get(imeiAndDate) > 30 &&
                        parameterB22.get(imeiAndDate) == 1) {
                    mode = m5;
                    break;
                }
                //расчет режимов для ЗУК ACROS, VECTOR, PCM
            case 6:
            case 8:
            case 9:
            case 10:
            case 11:
            case 12:
            case 13:
            case 14:
            case 15:
            case 16:
                if (parameterP12.get(imeiAndDate) < 500 &&
                        parameterP11.get(imeiAndDate) > 0.5 &&
                        parameterP45.get(imeiAndDate) < 30 &&
                        parameterB22.get(imeiAndDate) != 1) {
                    mode = m1;
                    break;
                }
                if (parameterP12.get(imeiAndDate) <= 500 &&
                        parameterP11.get(imeiAndDate) <= 0.5) {
                    mode = m2;
                    break;
                }
                if (parameterP12.get(imeiAndDate) > 500 &&
                        parameterP11.get(imeiAndDate) <= 0.5 &&
                        parameterB22.get(imeiAndDate) != 1) {
                    mode = m3;
                    break;
                }
                if (parameterP12.get(imeiAndDate) > 500 &&
                        parameterP11.get(imeiAndDate) > 0.5 &&
                        parameterP45.get(imeiAndDate) > 30 &&
                        parameterB22.get(imeiAndDate) == 1) {
                    mode = m4;
                    break;
                }
                if (parameterP12.get(imeiAndDate) > 500 &&
                        parameterP11.get(imeiAndDate) <= 0.5 &&
                        parameterP45.get(imeiAndDate) > 30 &&
                        parameterB22.get(imeiAndDate) == 1) {
                    mode = m5;
                    break;
                }
                //расчет режимов для ЗУК TORUM
            case 17:
            case 18:
            case 19:
            case 20:
            case 21:
            case 25:
                if (parameterP12.get(imeiAndDate) < 500 &&
                        parameterP11.get(imeiAndDate) > 0.5 &&
                        parameterP15.get(imeiAndDate) < 30 &&
                        parameterB22.get(imeiAndDate) != 1) {
                    mode = m1;
                    break;
                }
                if (parameterP12.get(imeiAndDate) <= 500 &&
                        parameterP11.get(imeiAndDate) <= 0.5) {
                    mode = m2;
                    break;
                }
                if (parameterP12.get(imeiAndDate) > 500 &&
                        parameterP11.get(imeiAndDate) <= 0.5 &&
                        parameterB22.get(imeiAndDate) != 1) {
                    mode = m3;
                    break;
                }
                if (parameterP12.get(imeiAndDate) > 500 &&
                        parameterP11.get(imeiAndDate) > 0.5 &&
                        parameterP15.get(imeiAndDate) > 30 &&
                        parameterB22.get(imeiAndDate) == 1) {
                    mode = m4;
                    break;
                }
                if (parameterP12.get(imeiAndDate) > 500 &&
                        parameterP11.get(imeiAndDate) <= 0.5 &&
                        parameterP15.get(imeiAndDate) > 30 &&
                        parameterB22.get(imeiAndDate) == 1) {
                    mode = m5;
                    break;
                }
            default:
                mode = m0;
                break;
        }
        return mode;
    }

    //добавление объетов Sensor в БД
    public void insertBatchSensor(final List<Sensor> sensors, JdbcTemplate jdbcTemplateAgrotronic) {
        long startInsertSensors = System.currentTimeMillis();
        BatchPreparedStatementSetter statementSetter = new BatchPreparedStatementSetter() {
            @Override
            public void setValues(PreparedStatement ps, int i) throws SQLException {
                Sensor sensor = sensors.get(i);
                ps.setLong(1, sensor.getId_old());
                ps.setLong(2, sensor.getImei());
                ps.setTimestamp(3, Timestamp.valueOf(sensor.getDate()));
                ps.setString(4, sensor.getParam_name());
                ps.setString(5, sensor.getValue());
            }

            @Override
            public int getBatchSize() {
                return sensors.size();
            }
        };

        jdbcTemplateAgrotronic.batchUpdate("INSERT INTO sensor_history(id_old, imei, date, param_name, value) VALUES (?, ?, ?, ?, ?)", statementSetter);
        long finishInsertSensors = System.currentTimeMillis();
        long insertSensorsTime = finishInsertSensors - startInsertSensors;
        System.out.println("Insert sensors time = " + insertSensorsTime +
                "ms. Added " + sensors.size() + " records. Recording speed " + ((double) sensors.size() / insertSensorsTime) * 1000 + " sensor/s");
    }

    //возвращает набор объектов Sensor
    public List<Sensor> getSensors(List<Messages> messagesList) {
        List<Sensor> sensorsAll = new ArrayList<>();
        for (int i = 0; i < messagesList.size(); i++) {
            sensorsAll.addAll(sensorsParser(messagesList.get(i)));
        }
        return sensorsAll;
    }

    //парсинг набора параметров сообщения
    public List<Sensor> sensorsParser(Messages messages) {
        List<Sensor> sensors = new ArrayList<>();
        long idMessage = messages.getId();
        long imei = messages.getImei();
        LocalDateTime timestamp = messages.getTimestamp_create();
        String parameters = messages.getParams();
        if (parameters != null && !parameters.trim().isEmpty()) {
            String[] params = parameters.split(",");

            for (int j = 0; j < params.length; j++) {
                String[] parameter = params[j].split(":");
                if (parameter.length == 3) {
                    String param_name = parameter[0];

                    //проверка длины строки
                    if (param_name.length() > 10) param_name = param_name.substring(0, 10);
                    String param_value = parameter[2];

                    //проверка длины строки
                    if (param_value.length() > 15) param_value = param_value.substring(0, 15);
                    sensors.add(new Sensor(idMessage, imei, timestamp, param_name, param_value));
                }
            }
        }
        return sensors;
    }

    //опеределение режима работы на основе валидированных параметров
    public int modeOfSensor(long unitType, long imei, LocalDateTime date, JdbcTemplate jdbcTemplateAgrotronic) {
        int mode;

        //запрос с БД
        String sql = "WITH sens AS (SELECT id, name, param AS param_name, valid_period_seconds, default_val, id_unit, unit_type FROM sensors) " +
                "SELECT t2.param_name:: text, (Select CASE WHEN t2.valid_period_seconds is not null then (WITH values as\n" +
                "                               (SELECT t1.value FROM public.sensor_history t1 where t1.imei = " + imei +
                "                                and t1.date <= '" + date + "'\n" +
                "                                and t1.date >= (timestamp '" + date + "' - CASE \n" +
                "                                WHEN( make_interval (secs => t2.valid_period_seconds)) is not null \n" +
                "                                THEN ( make_interval (secs => t2.valid_period_seconds) )\n" +
                "                                ELSE interval '0' end) and t1.param_name = t2.param_name order by date desc limit 1)\n" +
                "                                Select CASE WHEN (SELECT tt1.value  FROM values tt1)is not null\n" +
                "                                       THEN  ((SELECT tt2.value  FROM values tt2) ::text)\n" +
                "                                       ELSE ((Select default_val from sens s where s.id = t2.id )::text) END)\n" +
                "                                       else (SELECT t1.value FROM public.sensor_history t1\n" +
                "                                       where t1.imei = " + imei + " and t1.date <= '" + date + "' and t1.param_name = t2.param_name order by date desc limit 1)\n" +
                "                               end):: text  as value FROM sens t2 where t2.id_unit = (SELECT tt3.id  FROM public.units tt3 where tt3.imei = " + imei + ") or \n" +
                "                                       t2.unit_type = (SELECT tt4.unit_type  FROM public.units tt4 where tt4.imei = " + imei + ") and t2.param_name is not null";

        List<Sensor> sensorList = new ArrayList<>();
        //достаем из запроса необходимые параметры
        List<Map<String, Object>> rows = jdbcTemplateAgrotronic.queryForList(sql);
        for (Map row : rows) {
            Sensor sensor = new Sensor();
            String param_name = (String) row.get("param_name");
            if (param_name.equals("P12") ||
                    param_name.equals("P11") ||
                    param_name.equals("P50") ||
                    param_name.equals("B22") ||
                    param_name.equals("P45") ||
                    param_name.equals("PC4") ||
                    param_name.equals("PD7") ||
                    param_name.equals("PA5&1") ||
                    param_name.equals("PA3&64") ||
                    param_name.equals("PA4&4") ||
                    param_name.equals("PC7") ||
                    param_name.equals("P50")) {
                sensor.setParam_name(param_name);
                sensor.setValue((String) row.get("value"));
                sensorList.add(sensor);
            }
        }

        //создаем карту параметров
        Map<String, Double> sensorMap = new HashMap<>();
        for (Sensor s : sensorList) {
            sensorMap.put(s.getParam_name(), Double.parseDouble(s.getValue()));
        }

        //на основе типа техники опеределяем форумулу расчета режима
        switch (toIntExact(unitType)) {
            //расчет режима для КУС
            case 2:
                if (!(sensorMap.get("PD7") >= 500)){
                    mode = 2;
                    break;
                }
                if (!(sensorMap.get("PC4") * 0.2 >= 0.5)){
                    mode = 3;
                    break;
                }
                if (sensorMap.get("PA5&1") < 1 &&
                        sensorMap.get("PA3&64") == 64 ||
                        sensorMap.get("PA4&4") == 4){
                    mode = 4;
                    break;
                }
                if (!(sensorMap.get("PC4") * 0.2 >= 0.5) &&
                        sensorMap.get("PA5&1") < 1 &&
                        sensorMap.get("PA3&64") == 64 ||
                        sensorMap.get("PA4&4") == 4){
                    mode = 5;
                    break;
                }
                if (sensorMap.get("PC4") * 0.2 >= 0.5){
                    mode = 1;
                    break;
                }
            //расчет режимов для КУК
            case 3:
            case 4:
            case 5:
                if (!(sensorMap.get("P12") >= 500)) {
                    mode = 2;
                    break;
                }
                if (!(sensorMap.get("P11") >= 0.5)) {
                    mode = 3;
                    break;
                }
                if (sensorMap.get("P50") >= 30 &&
                        sensorMap.get("B22") == 1) {
                    mode = 4;
                    break;
                }
                if (!(sensorMap.get("P11") >= 0.5) &&
                        sensorMap.get("P50") >= 30 &&
                        sensorMap.get("B22") == 1) {
                    mode = 5;
                    break;
                }
                if (sensorMap.get("P11") >= 0.5) {
                    mode = 1;
                    break;
                }
                //расчет режимов для ЗУК ACROS 550, VECTOR 410, NOVA
            case 7:
            case 24:
            case 26:
                if (!(sensorMap.get("PD7") >= 500)) {
                    mode = 2;
                    break;
                }
                if (!(sensorMap.get("PC4") * 0.2 >= 0.5)) {
                    mode = 3;
                    break;
                }
                if (sensorMap.get("PC7") * 10 >= 30 &&
                        sensorMap.get("PA3&64") == 64) {
                    mode = 4;
                    break;
                }
                if (!(sensorMap.get("PC4") * 0.2 >= 0.5) &&
                        sensorMap.get("PC7") * 10 >= 30 &&
                        sensorMap.get("PA3&64") == 64) {
                    mode = 5;
                    break;
                }
                if (sensorMap.get("PC4") * 0.2 >= 0.5) {
                    mode = 1;
                    break;
                }
                //расчет режимов для остальных ЗУК ACROS, VECTOR
            case 6:
            case 8:
            case 9:
            case 10:
            case 11:
            case 12:
            case 13:
            case 14:
            case 15:
            case 16:
                if (!(sensorMap.get("P12") >= 500)) {
                    mode = 2;
                    break;
                }
                if (!(sensorMap.get("P11") >= 0.5)) {
                    mode = 3;
                    break;
                }
                if (sensorMap.get("P45") >= 30 &&
                        sensorMap.get("B22") == 1) {
                    mode = 4;
                    break;
                }
                if (!(sensorMap.get("P11") >= 0.5) &&
                        sensorMap.get("P45") >= 30 &&
                        sensorMap.get("B22") == 1) {
                    mode = 5;
                    break;
                }
                if (sensorMap.get("P11") >= 0.5) {
                    mode = 1;
                    break;
                }
                //расчет режимов для ЗУК TORUM
            case 17:
            case 18:
            case 19:
            case 20:
            case 21:
            case 25:
                if (!(sensorMap.get("P12") >= 500)) {
                    mode = 2;
                    break;
                }
                if (!(sensorMap.get("P11") >= 0.5)) {
                    mode = 3;
                    break;
                }
                if (sensorMap.get("P15") >= 30 &&
                        sensorMap.get("B22") == 1) {
                    mode = 4;
                    break;
                }
                if (!(sensorMap.get("P11") >= 0.5) &&
                        sensorMap.get("P15") >= 30 &&
                        sensorMap.get("B22") == 1) {
                    mode = 5;
                    break;
                }
                if (sensorMap.get("P11") >= 0.5) {
                    mode = 1;
                    break;
                }
            default:
                mode = 0;
                break;
        }
        return mode;
    }
}
