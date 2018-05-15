package etl.DTO;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
// режим работы
public class Mode {
    String name;
    String color;
    long id_model;
    String comment;
}
