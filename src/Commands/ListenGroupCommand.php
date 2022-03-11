<?php

namespace Prwnr\Streamer\Commands;

use Exception;
use Illuminate\Console\Command;
use Illuminate\Contracts\Container\BindingResolutionException;
use Prwnr\Streamer\Contracts\Archiver;
use Prwnr\Streamer\Contracts\Errors\MessagesFailer;
use Prwnr\Streamer\Contracts\MessageReceiver;
use Prwnr\Streamer\EventDispatcher\ReceivedMessage;
use Prwnr\Streamer\EventDispatcher\StreamMessage;
use Prwnr\Streamer\EventDispatcher\GroupStreamer;
use Prwnr\Streamer\GroupStream;
use Prwnr\Streamer\ListenersStack;
use Prwnr\Streamer\Stream;
use Symfony\Component\Console\Input\InputArgument;
use Symfony\Component\Console\Input\InputOption;
use Throwable;

/**
 * Class ListenCommand.
 */
class ListenGroupCommand extends Command
{
    /**
     * The console command name.
     *
     * @var string
     */
    protected $name = 'streamer:listenGroup';

    /**
     * The console command description.
     *
     * @var string
     */
    protected $description = 'RedisStream listen command that awaits for new messages on given Stream and fires local events based on streamer configuration';

    private GroupStreamer $groupStreamer;
    private MessagesFailer $failer;
    private ?int $maxAttempts;
    private Archiver $archiver;
    private string $consumer;

    /**
     * ListenCommand constructor.
     *
     * @param  Streamer  $streamer
     * @param  MessagesFailer  $failer
     * @param  Archiver  $archiver
     */
    public function __construct(GroupStreamer $groupStreamer, MessagesFailer $failer, Archiver $archiver)
    {
        $this->groupStreamer = $groupStreamer;
        $this->failer = $failer;
        $this->archiver = $archiver;

        parent::__construct();
    }

    /**
     * Execute the console command.
     *
     * @throws Throwable
     */
    public function handle(): int
    {
        $this->groupStreamer->asStream($this->argument('stream'));

        $listeners = ListenersStack::all();
        $this->error(json_encode($listeners));

        $stream = new Stream($this->argument('stream'));
        $this->setupGroupListening($stream);

        while(true) {
            $readGroup = $stream->readGroup($this->argument('group'), $this->consumer);
            if(isset($readGroup[$stream->getName()])) {
                ksort($readGroup[$stream->getName()]);
                array_walk($readGroup[$stream->getName()], function($content, $id) use($listeners) {
                    $message = new ReceivedMessage($id, $content);
                    $localListeners = $listeners[$content['name']] ?? null;

                    if (!$localListeners) {
                        $this->error("There are no local listeners associated with ".$content['name']." event in configuration.");
                    } else {
                        $failed = false;
                        foreach ($localListeners as $listener) {
                            $receiver = app()->make($listener);
                            if (!$receiver instanceof MessageReceiver) {
                                $this->error("Listener class [$listener] needs to implement MessageReceiver");
                                continue;
                            }

                            try {
                                $receiver->handle($message);
                            } catch (Throwable $e) {
                                $failed = true;
                                report($e);

                                $this->printError($message, $listener, $e);
                                $this->failer->store($message, $receiver, $e);

                                continue;
                            }

                            $this->printInfo($message, $listener);
                        }

                        if ($failed) {
                            return;
                        }

                        if ($this->option('archive')) {
                            $this->archive($message);
                        }

                        if (!$this->option('archive') && $this->option('purge')) {
                            $this->purge($message);
                        }
                    }
                });
            }
            sleep(1);
        }

        return 0;
    }

    /**
     * @param  Stream  $stream
     */
    private function setupGroupListening(Stream $stream): void
    {
        if (!$stream->groupExists($this->argument('group'))) {
            $stream->createGroup($this->argument('group'));
            $this->info("Created new group: {$this->argument('group')} on a stream: {$this->argument('stream')}");
        }

        $this->consumer = $this->argument('group').'-'.time();

        if ($this->option('reclaim')) {
            $this->reclaimMessages($stream, $this->consumer);
        }

        $this->groupStreamer->asConsumer($this->consumer, $this->argument('group'));
    }

    /**
     * @param  Stream  $stream
     * @param  string  $consumerName
     */
    private function reclaimMessages(Stream $stream, string $consumerName): void
    {
        $pendingMessages = $stream->pending($this->argument('group'));

        $messages = [];
        foreach ($pendingMessages as $message) {
            $messages[] = $message[0];
        }

        if (!$messages) {
            return;
        }

        $consumer = new Stream\Consumer($consumerName, $stream, $this->argument('group'));
        $consumer->claim($messages, $this->option('reclaim'));
    }

    /**
     * Removes message from the stream and stores message in DB.
     * No verification for other consumers is made in this archiving option.
     *
     * @param  ReceivedMessage  $message
     */
    private function archive(ReceivedMessage $message): void
    {
        try {
            $this->archiver->archive($message);
            $this->info("Message [{$message->getId()}] has been archived from the '{$message->getEventName()}' stream.");
        } catch (Exception $e) {
            $this->warn("Message [{$message->getId()}] from the '{$message->getEventName()}' stream could not be archived. Error: ".$e->getMessage());
        }
    }

    /**
     * Removes message from the stream.
     * No verification for other consumers is made in this purging option.
     *
     * @param  ReceivedMessage  $message
     */
    private function purge(ReceivedMessage $message): void
    {
        $stream = new Stream($this->argument('stream'));
        $result = $stream->delete($message->getId());
        if ($result) {
            $this->info("Message [{$message->getId()}] has been purged from the '{$message->getEventName()}' stream.");
        }
    }

    /**
     * @param  ReceivedMessage  $message
     * @param  string  $listener
     */
    private function printInfo(ReceivedMessage $message, string $listener): void
    {
        $this->info(sprintf(
            "Processed message [%s] on '%s' stream by [%s] listener.",
            $message->getId(),
            $message->getEventName(),
            $listener
        ));
    }

    /**
     * @param  ReceivedMessage  $message
     * @param  string  $listener
     * @param  Exception  $e
     */
    private function printError(ReceivedMessage $message, string $listener, Exception $e): void
    {
        $this->error(sprintf(
            "Listener error. Failed processing message with ID %s on '%s' stream by %s. Error: %s",
            $message->getId(),
            $message->getEventName(),
            $listener,
            $e->getMessage()
        ));
    }

    /**
     * @inheritDoc
     */
    protected function getArguments(): array
    {
        return [
            [
                'stream',
                InputArgument::REQUIRED,
                'Name of the stream'
            ],
            [
                'group',
                InputArgument::REQUIRED,
                'Name of the group we should listen to'
            ],
        ];
    }
    /**
     * @inheritDoc
     */
    protected function getOptions(): array
    {
        return [
            [
                'consumer', null, InputOption::VALUE_REQUIRED,
                'Name of your group consumer. If not provided a name will be created as groupname-timestamp'
            ],
            [
                'reclaim', null, InputOption::VALUE_REQUIRED,
                'Milliseconds of pending messages idle time, that should be reclaimed for current consumer in this group. Can be only used with group listening'
            ],
            [
                'purge', null, InputOption::VALUE_NONE,
                'Will remove message from the stream if it will be processed successfully by all listeners in the current stack.'
            ],
            [
                'archive', null, InputOption::VALUE_NONE,
                'Will remove message from the stream and store it in database if it will be processed successfully by all listeners in the current stack.'
            ],
        ];
    }
}
