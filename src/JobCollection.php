<?php

namespace dizard;

use parallel\{Channel, Runtime, Events, Events\Event, Future};


class JobCollection{
    private $poolJob;
    private $Pool;
    private $channelName;
    private $listPid = [];

    public function __construct(Pool $Pool)
    {
        $this->poolJob = new \SplObjectStorage();
        $this->Pool = $Pool;
        $this->channelName = uniqid();
    }

    /**
     * @param \Closure $closure
     * @param $args
     * @return $this
     */
    public function addJob(\Closure $closure, $args) {
        $this->poolJob[$closure] = $args;
        return $this;
    }

    /**
     * @return $this
     */
    public function run() {
        Channel::make($this->channelName, count($this->poolJob));
        foreach ($this->poolJob as $Closure) {
            $pid = $this->Pool->addJob($Closure, $this->poolJob[$Closure], $this->channelName);
            $this->listPid[$pid] = null;
        }
        return $this;
    }

    public function waitCompleteAndGetResultsAll() {
        $out = [];
        $channel = Channel::open($this->channelName);
        while ($res = $channel->recv()) {
            $out[$res['pid']] = $res['val'];
            unset($this->listPid[$res['pid']]);
            if (count($this->listPid)===0) break;
        }
        return $out;
    }

    public function GetResultsAll() {
        return (new Runtime())->run(function($channelName, $listPid) {
            $out = [];
            $channel = Channel::open($channelName);
            while ($res = $channel->recv()) {
                $out[$res['pid']] = $res['val'];
                unset($listPid[$res['pid']]);
                if (count($listPid)===0) break;
            }
            return $out;
        }, [$this->channelName, $this->listPid]);
    }

    public function waitCompleteAndGetResults() {
        $channel = Channel::open($this->channelName);
        while ($res = $channel->recv()) {
            unset($this->listPid[$res['pid']]);
            if (count($this->listPid)===0) return ;
            yield $res;
        }
    }
}