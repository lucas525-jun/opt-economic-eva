package mx.com.innovating.cloud.data.models;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.time.LocalDateTime;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class FechaInicioResult {
    private LocalDateTime fechaEntrada;
    private Double mes;
    private Double anio;
    private LocalDateTime fechaInicio;
    private LocalDateTime fechaTermino;
    private Integer nEquipo;
    private Integer nPozos;
    private Integer entrada;
    private Double diasPerf;
    private Double diasTerm;
    private Double dias;
    private Integer id;
}