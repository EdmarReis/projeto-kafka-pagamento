package edmar.projeto.rest.kafka.data;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Component
public class PedidoData {

    private String id;
    private String cliente;
    private String produto;
    private BigDecimal valor;
    private boolean pago;
    //private boolean separado;

}