<?php

namespace dizard;

use parallel\{Channel, Runtime, Events, Events\Event, Future};

class Pool
{
    private $size = 4;
    private $sizeResult = 1000;
    private $channel;
    private $channelOut;
    private $counter = 0;
    private $sheduler;
    private $shedulerFuture;
    private $bootstrap;
    private $nameJob = [];
    private $channelNameAddJob = 'eventJobs';

    private $shutdownCallbacks = [];

    /**
     * Pool constructor.
     * @param int $size
     * @param null $bootstrap
     * @throws \Exception
     */
    public function __construct($size = 4, $bootstrap = null)
    {
        if ($bootstrap && !file_exists($bootstrap)) {
            throw new \Exception('not found boostrap: ' . $bootstrap);
        }

        $this->channelNameAddJob = uniqid();

        $this->size = $size;
        $this->channel = Channel::make($this->channelNameAddJob, Channel::Infinite);
        $this->channelOut = new Channel();
        $this->bootstrap = $bootstrap;

        pcntl_signal(SIGTERM, [$this, 'shutdown']);
        register_shutdown_function([$this, 'shutdown']);

        $this->scheduler();
    }

    private function scheduler()
    {
        $this->sheduler = new Runtime();
        $this->shedulerFuture = $this->sheduler->run(function (Channel $channel, Channel $channelOut, $limit = 4, $bootstrap) {
            $events = new Events();
            $events->addChannel($channel);
            $events->setTimeout(86400 * 1000000);
            //$events->setBlocking(false);

            $counterActiveJobs = 0;
            $listJobWaitEngine = [];
            $listJobInEngine = [];

            $needSendResult = false;
            $waitComplete = false;
            $poolChannelAnswer = [];
            $result = [];

            $poolWorkers = new \SplObjectStorage();
            $poolFuture = new \SplObjectStorage();

            $getWorker = function () use ($poolWorkers, $limit, $bootstrap) {
                foreach ($poolWorkers as $runtime) {
                    if ($poolWorkers[$runtime]->done()) {
                        return $runtime;
                    }
                }
                if (count($poolWorkers) >= $limit) return false;
                $runtime = $bootstrap ? new Runtime($bootstrap) : new Runtime();
                $poolWorkers->attach($runtime);
                return $runtime;
            };

            $jobDone = function ($id, $value) use (&$counterActiveJobs, &$result, &$poolChannelAnswer, &$listJobInEngine) {
                unset($listJobInEngine[$id]);
                $counterActiveJobs--;
                if ($value instanceof \Exception) {
                    echo "{$id} ERROR: " . $value->getCode() . "\n";
                    echo "{$id} ERROR: " . $value->getFile() . ", line {$value->getLine()}" . "\n";
                }
                if ($value) {
                    $result[$id] = is_object($value) ? serialize($value) : $value;
                }
                if (isset($poolChannelAnswer[$id])) {
                    Channel::open($poolChannelAnswer[$id])->send(['pid' => $id, 'val' => $result[$id]]);
                }
            };

            while (true) {
                pcntl_signal_dispatch();
                usleep(100);
                if ($waitComplete && $counterActiveJobs === 0) {
                    $waitComplete = false;
                    $channelOut->send($needSendResult ? $result : '');
                    if ($needSendResult) {
                        $result = [];
                        $needSendResult = false;
                    }
                }
                if (count($listJobInEngine) < $limit) {
                    foreach ($listJobWaitEngine as $n => $data) {
                        if ($runtime = $getWorker()) {
                            $future = $runtime->run($data['call'], $data['args']);
                            $poolWorkers->attach($runtime, $future);
                            $poolFuture->attach($future, $runtime);
                            $listJobInEngine[$n] = 1;
                            if ($data['channelAnswer']) $poolChannelAnswer[$n] = $data['channelAnswer'];
                            $events->addFuture($n, $future);
                            unset($listJobWaitEngine[$n]);
                        }
                    }
                }
                if (!$event = $events->poll()) continue;
                switch ($event->type) {
                    case Event\Type::Close:
                        return;
                    case Event\Type::Error:
                        if ($event->object instanceof Future) {
                            $runtime = $poolFuture->offsetGet($event->object);
                            $runtime->close();
                            $poolWorkers->detach($runtime);
                            $poolFuture->detach($event->object);
                        }
                        $jobDone($event->source, $event->value);
                        break;
                    case Event\Type::Read:
                        if ($event->object instanceof Future) {
                            $poolFuture->detach($event->object);
                            $jobDone($event->source, $event->value);
                        } elseif ($event->object instanceof Channel) {
                            $events->addChannel($channel);
                            $data = $event->value;
                            switch ($data['type']) {
                                case 'in':
                                    $counterActiveJobs++;
                                    $listJobWaitEngine[$data['id']] = $data;
                                    break;
                                case 'close':
                                    foreach ($poolWorkers as $runtime) $runtime->close();
                                    return;
                                case 'getResultAll':
                                    $waitComplete = true;
                                    $needSendResult = true;
                                    break;
                                case 'waitComplete':
                                    $waitComplete = true;
                                    break;
                                case 'jobIsComplete':
                                    $isComplete = !array_key_exists($data['id'], $listJobInEngine) && !array_key_exists($data['id'], $listJobWaitEngine);
                                    $channelOut->send($isComplete);
                                    break;
                                case 'getCountActiveJob':
                                    $channelOut->send($counterActiveJobs);
                                    break;
                                case 'clear':
                                    $result = [];
                                    gc_collect_cycles();
                                    break;
                            }
                        }
                        break;
                    case Event\Type::Write:
                        $events->addChannel($channel);
                        break;
                }
            }
        }, [$this->channel, $this->channelOut, $this->size, $this->bootstrap]);
    }

    public function addJob(\Closure $call, $args = [], $channelAnswer = null): int
    {
        $this->channel->send(['type' => 'in', 'call' => $call->bindTo(null), 'args' => $args, 'id' => ++$this->counter, 'channelAnswer' => $channelAnswer]);
        return $this->counter;
    }

    /**
     * @param string $uniName
     * @param \Closure $call
     * @param array $args
     * @param null $channelAnswer
     * @return false|int
     */
    public function addNameJob(string $uniName, \Closure $call, $args = [], $channelAnswer = null)
    {
        if (isset($this->nameJob[$uniName])) {
            $this->channel->send(['type' => 'jobIsComplete', 'id' => $this->nameJob[$uniName]]);
            $completed = $this->channelOut->recv();
            if (!$completed) return false;
        }
        return $this->nameJob[$uniName] = $this->addJob($call, $args, $channelAnswer);
    }

    public function shutdown()
    {
        if ($this->sheduler) {
            $this->channel->send(['type' => 'close']);
            $this->sheduler->close();
        }
    }

    public function getCountActiveJob()
    {
        $this->channel->send(['type' => 'getCountActiveJob']);
        return $this->channelOut->recv();
    }

    public function waitCompleteAndGetResults()
    {
        $this->channel->send(['type' => 'getResultAll']);
        return $this->channelOut->recv();
    }

    public function waitComplete() {
        $this->channel->send(['type' => 'waitComplete']);
        $this->channelOut->recv();
        return $this;
    }

    public function clear()
    {
        $this->channel->send(['type' => 'clear']);
        return $this;
    }
}