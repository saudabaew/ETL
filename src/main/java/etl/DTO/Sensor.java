package etl.DTO;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.time.LocalDateTime;

@Data
@AllArgsConstructor
public class Sensor {
    long id_old, imei;
    LocalDateTime date;
    String param_name, value;

    public Sensor() {
    }
}
