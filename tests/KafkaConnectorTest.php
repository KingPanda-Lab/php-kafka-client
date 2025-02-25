<?php

namespace Test;

use ByJG\MessageQueueClient\Connector\ConnectorFactory;
use ByJG\MessageQueueClient\Connector\ConnectorInterface;

use ByJG\MessageQueueClient\Envelope;
use ByJG\MessageQueueClient\Kafka\Topic;
use ByJG\MessageQueueClient\Message;
use ByJG\MessageQueueClient\Kafka\KafkaConnector;
use PHPUnit\Framework\TestCase;

class KafkaConnectorTest extends TestCase
{
    protected ConnectorInterface $connection;

    public function setUp(): void
    {
        $host = getenv('KAFKA_HOST');
        if (empty($host)) {
            $host = "localhost:9092";
        }

        ConnectorFactory::registerConnector(KafkaConnector::class);
        $this->connection = ConnectorFactory::create("kafka://$host");
    }

    public function clearTopic(string $topic): void
    {
        $kafkaContainerName = 'php-kafka-client-kafka-1';

        $command = sprintf(
            'docker exec %s kafka-topics --delete --topic %s --bootstrap-server %s',
            escapeshellarg($kafkaContainerName),
            escapeshellarg($topic),
            escapeshellarg('localhost:9092')
        );

        $output = [];
        $returnVar = 0;
        exec($command, $output, $returnVar);

        if ($returnVar !== 0 ) {
            throw new \Error("Failed to delete topic $topic: " . implode("\n", $output));
        }
    }

    public function testPublishConsume()
    {
        if ($this->topicExists("test-module")) {
            $this->clearTopic("test-module");
        }

        $topic = new Topic("test-module");
        $message = new Message("This is a test message to check if this method is working correctly");
        $this->connection->publish(new Envelope($topic, $message));

        $this->connection->consume($topic, function (Envelope $envelope) {
            $this->assertEquals("This is a test message to check if this method is working correctly", $envelope->getMessage()->getBody());
            $this->assertEquals("test-module", $envelope->getPipe()->getName());
            $this->assertEquals("test-module", $envelope->getPipe()->getProperty(KafkaConnector::TOPIC));
            $this->assertEquals([
                'topic' => 'test-module',
                'partition' => 0,
                'offset' => 0
            ], $envelope->getMessage()->getProperties());
            $this->assertEquals([
                'topic' => 'test-module',
                'partition' => -1
            ], $envelope->getPipe()->getProperties());
            return Message::ACK | Message::EXIT;
        }, function (Envelope $envelope, $ex) {
            throw $ex;
        });
    }

    public function testPublishConsumeRequeue()
    {
        if($this->topicExists("test")) {
            $this->clearTopic("test");
        }

        $topic = new Topic("test");
        $message = new Message("body_requeue");
        $this->connection->publish(new Envelope($topic, $message));

        $this->connection->consume($topic, function (Envelope $envelope) {
            $this->assertEquals("body_requeue", $envelope->getMessage()->getBody());
            $this->assertEquals("test", $envelope->getPipe()->getName());
            $this->assertEquals("test", $envelope->getPipe()->getProperty(KafkaConnector::TOPIC));
            $this->assertEquals([
                'topic' => 'test',
                'partition' => 0,
                'offset' => 0
            ], $envelope->getMessage()->getProperties());
            $this->assertEquals([
                'topic' => 'test',
                'partition' => -1
            ], $envelope->getPipe()->getProperties());
            return Message::REQUEUE | Message::EXIT;
        }, function (Envelope $envelope, $ex) {
            throw $ex;
        });
    }

    public function testPublishConsumeWithDlq()
    {
        if($this->topicExists("test2")) {
            $this->clearTopic("test2");
        }

        if($this->topicExists("dlq_test2")) {
            $this->clearTopic("dlq_test2");
        }

        $topic = new Topic("test2");
        $dlqTopic = new Topic("dlq_test2");
        $topic->withDeadLetter($dlqTopic);

        // Post and consume a message
        $message = new Message("bodydlq");
        $this->connection->publish(new Envelope($topic, $message));

        $this->connection->consume($topic, function (Envelope $envelope) {
            $this->assertEquals("bodydlq", $envelope->getMessage()->getBody());
            $this->assertEquals("test2", $envelope->getPipe()->getName());
            $this->assertEquals([
                "topic" => "test2",
                "partition" => 0,
                "offset" => 0
            ], $envelope->getMessage()->getProperties());
            $this->assertEquals([
                "topic" => "test2",
                "partition" => -1
            ], $envelope->getPipe()->getProperties());
            return Message::ACK | Message::EXIT;
        }, function (Envelope $envelope, $ex) {
            throw $ex;
        });

        if($this->topicExists("bodydlq_2")) {
            $this->clearTopic("bodydlq_2");
        }

        // Post and reject  a message (NACK, to send to the DLQ)
        $message = new Message("bodydlq_2");
        $this->connection->publish(new Envelope($topic, $message));

        $this->connection->consume($topic, function (Envelope $envelope) {
            $this->assertEquals("bodydlq_2", $envelope->getMessage()->getBody());
            $this->assertEquals("test2", $envelope->getPipe()->getName());
            $this->assertEquals([
                "topic" => "test2",
                "partition" => 0,
                "offset" => 1,
            ], $envelope->getMessage()->getProperties());
            $this->assertEquals([
                "topic" => "test2",
                "partition" => -1
            ], $envelope->getPipe()->getProperties());
            return Message::NACK | Message::EXIT;
        }, function (Envelope $envelope, $ex) {
            throw $ex;
        });

        // Consume the DLQ
        $this->connection->consume($dlqTopic, function (Envelope $envelope) {
            $this->assertEquals("bodydlq_2", $envelope->getMessage()->getBody());
            $this->assertEquals("dlq_test2", $envelope->getPipe()->getName());
            $properties = $envelope->getMessage()->getProperties();
            $this->assertEquals([
                "topic" => "dlq_test2",
                "partition" => 0,
                "offset" => 0
            ], $properties);
            $this->assertEquals([
                "topic" => "dlq_test2",
                "partition" => -1
            ], $envelope->getPipe()->getProperties());
            return Message::NACK | Message::EXIT;
        }, function (Envelope $envelope, $ex) {
            throw $ex;
        });

    }

    public function topicExists(string $topic): bool
    {
        $producer = $this->connection->getProducer();
        $metadata = $producer->getMetadata(true, null, 60e3); // 60 segundos de timeout

        foreach ($metadata->getTopics() as $metadataTopic) {
            if ($metadataTopic->getTopic() === $topic) {
                return true;
            }
        }

        return false;
    }

}