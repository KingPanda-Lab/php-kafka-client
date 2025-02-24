<?php

namespace ByJG\MessageQueueClient\Kafka;

use ByJG\MessageQueueClient\Connector\Pipe;

class Topic extends Pipe
{

    public function __construct(string $topicName)
    {
        parent::__construct($topicName);
    }

}