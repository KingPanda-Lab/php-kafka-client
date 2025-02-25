<?php

namespace ByJG\MessageQueueClient\Kafka;

use ByJG\MessageQueueClient\Connector\ConnectorInterface;
use ByJG\MessageQueueClient\Connector\Pipe;
use ByJG\MessageQueueClient\Envelope;
use ByJG\MessageQueueClient\Message;
use ByJG\Util\Uri;
use Closure;
use Error;
use Exception;
use RdKafka\Conf;
use RdKafka\KafkaConsumer;
use RdKafka\Producer;


class KafkaConnector implements ConnectorInterface
{

    public const TOPIC = 'topic';
    public const PARTITION = 'partition';

    protected Conf $conf;
    protected ?Producer $producer = null;
    protected ?KafkaConsumer $consumer = null;
    protected Uri $uri;

    public function setUp(Uri $uri): void
    {
        $this->uri = $uri;
        $this->conf = new Conf();
        $this->conf->setErrorCb(function (Error $error) {
            throw $error;
        });
        $this->conf->set('bootstrap.servers', $this->uri->getHost());
    }

    public static function schema(): array
    {
        return ["kafka"];
    }

    public function getDriver(): Conf
    {
        return $this->conf;
    }

    public function getProducer(): Producer
    {
        if (!$this->producer) {
            $this->producer = new Producer($this->conf);
        }
        return $this->producer;
    }

    protected function getConsumer(): KafkaConsumer
    {
        if (!$this->consumer) {
            $this->consumer = new KafkaConsumer($this->conf);
        }
        return $this->consumer;
    }

    protected function preparePipe(Topic $topic): void
    {
        $topic->setProperty(self::TOPIC, $topic->getName());
        $topic->setProperty(self::PARTITION, RD_KAFKA_PARTITION_UA);

    }

    public function publish(Envelope $envelope): void
    {
        $topic = $envelope->getPipe();
        /** @var Topic $topic */
        $this->preparePipe($topic);
        $topicName = $topic->getProperty(self::TOPIC);
        $partition = $topic->getProperty(self::PARTITION);

        $producer = $this->getProducer();
        $topic = $producer->newTopic($topicName);

        $topic->produce($partition, 0, $envelope->getMessage()->getBody());
        $producer->flush(1000);
    }

    public function consume(Pipe $pipe, Closure $onReceive, Closure $onError, ?string $identification = null): void
    {
        $topic = clone $pipe;
        $topicName = $topic->getProperty(self::TOPIC, $topic->getName());

        $this->conf->set('auto.offset.reset', $this->uri->getQueryPart('offset_reset') ?? 'earliest');
        $this->conf->set('group.id', $this->uri->getQueryPart('group_id') ?? 'default-group');
        $this->conf->set('enable.auto.commit', 'false');
        $this->conf->set('fetch.min.bytes', '1');
        $this->conf->set('fetch.wait.max.ms', '10');

        $consumer = $this->getConsumer();
        $consumer->subscribe([$topicName]);

        try {
            while (true) {
                $message = $consumer->consume(100);

                if ($message->err) {
                    if ($message->err == RD_KAFKA_RESP_ERR__PARTITION_EOF) {
                        continue;
                    } elseif ($message->err == RD_KAFKA_RESP_ERR__TIMED_OUT) {
                        continue;
                    } else {
                        $result = $onError(new Envelope($topic, new Message($message->errstr())) , $message);
                        continue;
                    }
                }

                $msg = new Message($message->payload);
                $msg->withProperty('topic', $message->topic_name);
                $msg->withProperty('partition', $message->partition);
                $msg->withProperty('offset', $message->offset);

                $envelope = new Envelope($topic, $msg);
                try {
                    $result = $onReceive($envelope);
                } catch (Error|Exception $error) {
                    $result = $onError($envelope, $error);
                }

                if (($result & Message::ACK) == Message::ACK) {
                    $this->consumer->commit($message);
                }

                if (($result & Message::NACK) == Message::NACK && $pipe->getDeadLetter() !== null) {
                    $dlqEnvelope = new Envelope($topic->getDeadLetter(), new Message($envelope->getMessage()->getBody()));
                    $this->publish($dlqEnvelope);
                }

                if (($result & Message::REQUEUE) == Message::REQUEUE) {
                    $this->publish($envelope);
                }

                if (($result & Message::EXIT) == Message::EXIT) {
                    break;
                }
            }
        } catch (Exception | Error $ex) {
            $onError(new Envelope($topic, new Message($ex->getMessage())), $ex);
        }
    }

}
