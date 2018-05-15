package etl.DTO;


import lombok.Data;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.util.Date;

@Data
public class Messages implements Serializable {
    // Поля
    private Long id;
    private LocalDateTime timestamp_create;                          // отправлено MTU
    private Double lat;                                     // широта
    private Double lon;                                     // долгота
    private String params;                                  // значения датчиков
    private Double speed;
    private Long imei;                                      // код imei
    private Double hdop;
    private Integer input;
    private Integer output;
    private Integer satellites_count;
    private Double altitude;
    private Long flags;
    private Integer course;
    private Date timestamp_edit;                            // получено ТП
    private Long unit;

    public Messages() {
    }

    public Messages(Long id, LocalDateTime  timestamp_create, Long imei, Double speed, String params) {
        this.id = id;
        this.imei = imei;
        this.speed = speed;
        this.timestamp_create = timestamp_create;
        this.params = params;
    }

    public Messages(Long id, LocalDateTime timestamp_create, Double lat, Double lon, String params, Double speed, Long imei, Double hdop, Integer input, Integer output, Integer satellites_count, Double altitude, Long flags, Integer course, Date timestamp_edit, Long unit) {
        this.id = id;
        this.timestamp_create = timestamp_create;
        this.lat = lat;
        this.lon = lon;
        this.params = params;
        this.speed = speed;
        this.imei = imei;
        this.hdop = hdop;
        this.input = input;
        this.output = output;
        this.satellites_count = satellites_count;
        this.altitude = altitude;
        this.flags = flags;
        this.course = course;
        this.timestamp_edit = timestamp_edit;
        this.unit = unit;
    }

    // Информация
    @Override
    public String toString() {
        return new StringBuilder()
                .append("Table <messages> id=").append(getId()).append(", ")
                .append("lat=").append(getLat()).append(", ")
                .append("lon=").append(getLon()).append(", ")
                .append("timestamp_create=").append(getTimestamp_create()).append(", ")
                .append("imei=").append(getImei())
                .toString();
    }
}
