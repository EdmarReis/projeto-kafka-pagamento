package edmar.projeto.rest.kafka.controller;

import edmar.projeto.rest.kafka.adapters.in.entity.PedidoEntity;
import edmar.projeto.rest.kafka.adapters.in.repository.PedidoRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Optional;

@RestController
@RequiredArgsConstructor
@RequestMapping("/pagamento")
public class PedidoController {

    @Autowired
    PedidoRepository pedidoRepository;

    @GetMapping("/find/{id}")
    public ResponseEntity<Optional<PedidoEntity>> findById(@PathVariable final String id) {
        Optional<PedidoEntity> getPedido = pedidoRepository.findById(id);
        return ResponseEntity.ok().body(getPedido);
    }

}
