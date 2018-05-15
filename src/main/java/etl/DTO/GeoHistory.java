package etl.DTO;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.postgis.MultiLineString;

import java.time.LocalDateTime;

@Data
@AllArgsConstructor
public class GeoHistory {
    long idOld;
    long imei;
    LocalDateTime date;
    double lon;
    double lat;
    double speedGps;
    double speedSens;
    int course;
    long mode;

    public GeoHistory() {
    }
}
