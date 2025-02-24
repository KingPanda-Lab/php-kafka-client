<?php

$broker = "localhost:9092";
$topicName = "pedidos_novos";

if ($argc < 2) {
    die("Uso: php kafka.php <producer|consumer>\n");
}

$mode = $argv[1];

if ($mode === "producer") {
    $conf = new RdKafka\Conf();
    $conf->set('metadata.broker.list', $broker);

    $producer = new RdKafka\Producer($conf);
    $topic = $producer->newTopic($topicName);

    $mensagem = "Novo pedido: #" . rand(1000, 9999);
    $topic->produce(RD_KAFKA_PARTITION_UA, 0, $mensagem, "sao paulo");
    $producer->flush(1000);

    echo "✅ Mensagem enviada: $mensagem\n";
}

elseif ($mode === "consumer") {
    $conf = new RdKafka\Conf();
    $conf->set('group.id', 'meu_grupo_consumidor');
    $conf->set('metadata.broker.list', $broker);
    $conf->set('enable.auto.commit', 'true'); // Kafka salva automaticamente o último offset
    $conf->set('auto.offset.reset', 'earliest'); // Se não houver offset salvo, lê desde o início

    $consumer = new RdKafka\KafkaConsumer($conf);
    $consumer->subscribe([$topicName]);

    echo "📥 Aguardando mensagens...\n";

    while (true) {
        $message = $consumer->consume(1000);
        if ($message->err) {
            if ($message->err == RD_KAFKA_RESP_ERR__TIMED_OUT) {
                continue;
            }
            echo "⚠️ Erro: {$message->errstr()}\n";
        } else {
            echo "📩 Recebido: {$message->payload}\n";
        }
    }
}

else {
    die("Modo inválido! Use 'producer' ou 'consumer'\n");
}
