package edmar.projeto.rest.kafka.service;

import edmar.projeto.rest.kafka.data.PedidoData;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class KafkaServices {


    public  KafkaTemplate<Object, Object> template;
    private KafkaTemplate<String, PedidoData> kafkaTemplate;

    public <T> void devolverEventoPagamento(String topico, T dados) {

        template.send(topico, dados);
    }

    @Autowired
    public KafkaServices(KafkaTemplate<String, PedidoData> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void enviarProdutoParaKafka(PedidoData pedidoData) {
        kafkaTemplate.send("devolvePedidoPagamento", pedidoData);
        //kafkaTemplate.send("devolvePedido", pedidoData);
    }



}
