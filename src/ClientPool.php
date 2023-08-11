<?php

declare(strict_types=1);
/**
 * This file is part of OpenSwoole RPC.
 * @link     https://openswoole.com
 * @contact  hello@openswoole.com
 * @license  https://github.com/openswoole/grpc/blob/main/LICENSE
 */

namespace OpenSwoole\GRPC;

class ClientPool {

	public const DEFAULT_SIZE = 16;

	private $pool;
	private $size;
	private $num;
	private $used;
	private $factory;
	private $config;
	private $settings = [
		'force_reconnect' => false,
		'receive_timeout' => -1,
		'force_recreate' => false
	];

	public function __construct($factory, $config, int $size = self::DEFAULT_SIZE, $settings = []) {
		$this->pool = new \Swoole\Coroutine\Channel($this->size = $size);
		$this->num = 0;
		$this->factory = $factory;
		$this->config = $config;
		if (is_array($settings)) {
			$this->settings = array_merge($this->settings, $settings);
		}
	}

	public function fill(): void {
		while ($this->size > $this->num) {
			$this->make();
		}
	}

	public function get(float $timeout = -1) {
		if ($this->pool->isEmpty() && $this->num < $this->size) {
			go(function () {
				while (!$this->make()) {
					if (!$this->settings['force_recreate']) {
						break;
					}
					\co::sleep(0.5);
				}
			});
		}
		$conn = $this->pool->pop((int) $this->settings['receive_timeout'] > -1 ? (int) $this->settings['receive_timeout'] : $timeout);
		if ($conn) {
			$this->used++;
		}
		return $conn;
	}

	public function put($connection, $isNew = false): void {
		if ($this->pool === null) {
			return;
		}
		if ($connection !== null) {
			$this->pool->push($connection);
			if (!$isNew) {
				$this->used--;
			}
		} else {
			$this->num -= 1;
			$this->make();
		}
	}

	public function close() {
		if (!$this->pool) {
			return false;
		}
		while (1) {
			if ($this->used > 0) {
				\co::sleep(0.5);
				continue;
			}
			if (!$this->pool->isEmpty()) {
				$client = $this->pool->pop();
				$client->close();
			} else {
				break;
			}
		}
		$this->pool->close();
		$this->pool = null;
		$this->num = 0;
	}

	protected function make() {
		$this->num++;
		try {
			$client = $this->factory::make($this->config['host'], $this->config['port'], $this->settings)->connect();
			$this->put($client, true);
		} catch (\Exception $ex) {
			$this->num--;
			return false;
		}
		return true;
	}

}
