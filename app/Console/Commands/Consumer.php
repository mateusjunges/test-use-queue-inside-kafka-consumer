<?php

namespace App\Console\Commands;

use App\Jobs\TestJob;
use Illuminate\Console\Command;
use Junges\Kafka\Contracts\KafkaConsumerMessage;
use Junges\Kafka\Facades\Kafka;

class Consumer extends Command
{
    protected $signature = 'consume';

    public function handle(): int
    {
        $consumer = Kafka::createConsumer()
            ->subscribe('test_topic')
            ->withHandler(function (KafkaConsumerMessage $message) {
                dispatch(new TestJob($message->getBody()));
            })->withAutoCommit()
            ->build();

        $consumer->consume();

        return 0;
    }
}
