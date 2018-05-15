package etl.DTO;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
//машина
public class Unit {
    long imei;
    long unit_type;
}
