package edmar.projeto.rest.kafka.adapters.out.producer;

import edmar.projeto.rest.kafka.data.PedidoData;
import edmar.projeto.rest.kafka.service.KafkaServices;
import edmar.projeto.rest.kafka.service.PedidoService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class EnviarPedidoPagamento {

    private final KafkaServices kafkaServices;

    @Autowired
    PedidoService pedidoService;

    @Autowired
    public EnviarPedidoPagamento(KafkaServices kafkaServices) {
        this.kafkaServices = kafkaServices;
    }

    public void metodoQueEnviaProdutoParaKafka(PedidoData pedidoData) {
        kafkaServices.enviarProdutoParaKafka(pedidoData);
    }



}
