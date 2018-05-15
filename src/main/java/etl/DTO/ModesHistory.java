package etl.DTO;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.postgis.MultiLineString;

import java.time.LocalDateTime;

@Data
@AllArgsConstructor
public class ModesHistory {
    long imei;
    LocalDateTime timeStart;
    LocalDateTime timeEnd;
    MultiLineString line;
    long mode;
}
