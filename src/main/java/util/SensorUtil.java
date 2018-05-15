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
        long m0 = 0; long m1 = 1; long m2 = 2; long m3 = 3; long m4 = 4; long m5 = 5;
        long mode = 0;

        switch (unitType) {
            //расчет режимов для КУК
            case 3: case 4: case 5:
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
            case 6: case 8: case 9: case 10: case 11: case 12: case 13: case 14: case 15: case 16:
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
            case 17: case 18: case 19: case 20: case 21: case 25:
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

    public int modeOfSensor(long unitType, List<Sensor> sensors){
        int mode;

        Map<String, Double> sensorMap = new HashMap<>();
        for (Sensor s: sensors) {
            if (s.getParam_name().equals("P12") ||
                    s.getParam_name().equals("P11") ||
                    s.getParam_name().equals("P50") ||
                    s.getParam_name().equals("B22") ||
                    s.getParam_name().equals("P45") ||
                    s.getParam_name().equals("P50")) {
                sensorMap.put(s.getParam_name(), Double.parseDouble(s.getValue()));
            }
        }

        switch (toIntExact(unitType)) {
            //расчет режимов для КУК
            case 3: case 4: case 5:
                if (sensorMap.get("P12") < 500 &&
                        sensorMap.get("P11") < 0.5 &&
                        sensorMap.get("P50") < 30 &&
                        sensorMap.get("B22") != 1) {
                    mode = 1;
                    break;
                }
                if (sensorMap.get("P12") <= 500 &&
                        sensorMap.get("P11") <= 0.5) {
                    mode = 2;
                    break;
                }
                if (sensorMap.get("P12") > 500 &&
                        sensorMap.get("P11") <= 0.5 &&
                        sensorMap.get("B22") != 1) {
                    mode = 3;
                    break;
                }
                if (sensorMap.get("P12") > 500 &&
                        sensorMap.get("P11") > 0.5 &&
                        sensorMap.get("P50") > 30 &&
                        sensorMap.get("B22") == 1) {
                    mode = 4;
                    break;
                }
                if (sensorMap.get("P12") > 500 &&
                        sensorMap.get("P11") <= 0.5 &&
                        sensorMap.get("P50") > 30 &&
                        sensorMap.get("B22") == 1) {
                    mode = 5;
                    break;
                }
                //расчет режимов для ЗУК ACROS, VECTOR, PCM
            case 6: case 8: case 9: case 10: case 11: case 12: case 13: case 14: case 15: case 16:
                if (sensorMap.get("P12") < 500 &&
                        sensorMap.get("P11") > 0.5 &&
                        sensorMap.get("P45") < 30 &&
                        sensorMap.get("B22") != 1) {
                    mode = 1;
                    break;
                }
                if (sensorMap.get("P12") <= 500 &&
                        sensorMap.get("P11") <= 0.5) {
                    mode = 2;
                    break;
                }
                if (sensorMap.get("P12") > 500 &&
                        sensorMap.get("P11") <= 0.5 &&
                        sensorMap.get("B22") != 1) {
                    mode = 3;
                    break;
                }
                if (sensorMap.get("P12") > 500 &&
                        sensorMap.get("P11") > 0.5 &&
                        sensorMap.get("P45") > 30 &&
                        sensorMap.get("B22") == 1) {
                    mode = 4;
                    break;
                }
                if (sensorMap.get("P12") > 500 &&
                        sensorMap.get("P11") <= 0.5 &&
                        sensorMap.get("P45") > 30 &&
                        sensorMap.get("B22") == 1) {
                    mode = 5;
                    break;
                }
                //расчет режимов для ЗУК TORUM
            case 17: case 18: case 19: case 20: case 21: case 25:
                if (sensorMap.get("P12") < 500 &&
                        sensorMap.get("P11") > 0.5 &&
                        sensorMap.get("P15") < 30 &&
                        sensorMap.get("B22") != 1) {
                    mode = 1;
                    break;
                }
                if (sensorMap.get("P12") <= 500 &&
                        sensorMap.get("P11") <= 0.5) {
                    mode = 2;
                    break;
                }
                if (sensorMap.get("P12") > 500 &&
                        sensorMap.get("P11") <= 0.5 &&
                        sensorMap.get("B22") != 1) {
                    mode = 3;
                    break;
                }
                if (sensorMap.get("P12") > 500 &&
                        sensorMap.get("P11") > 0.5 &&
                        sensorMap.get("P15") > 30 &&
                        sensorMap.get("B22") == 1) {
                    mode = 4;
                    break;
                }
                if (sensorMap.get("P12") > 500 &&
                        sensorMap.get("P11") <= 0.5 &&
                        sensorMap.get("P15") > 30 &&
                        sensorMap.get("B22") == 1) {
                    mode = 5;
                    break;
                }
            default:
                mode = 0;
                break;
        }
        return mode;
    }
}
