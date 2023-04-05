<?php

declare(strict_types=1);
/**
 * This file is part of OpenSwoole RPC.
 * @link     https://openswoole.com
 * @contact  hello@openswoole.com
 * @license  https://github.com/openswoole/grpc/blob/main/LICENSE
 */

namespace OpenSwoole\GRPC;

use OpenSwoole\GRPC\Exception\ClientException;

class Client implements ClientInterface {

	private $client;
	private $streams;
	private $closed = false;
	private $host;
	private $port;
	private $mode;
	private $settings = [
		'timeout' => 3,
		'open_eof_check' => true,
		'package_max_length' => 2 * 1024 * 1024,
		'http2_max_concurrent_streams' => 1000,
		'http2_max_frame_size' => 2 * 1024 * 1024,
		'max_retries' => 10,
		'force_reconnect' => false,
		'receive_timeout' => -1
	];
	private $forced_reconnect = false;

	public function __construct($host, $port, $mode = Constant::GRPC_CALL, $settings = []) {
		// TODO: clientInterceptors
		$this->streams = [];
		$this->mode = $mode;
		$this->host = $host;
		$this->port = $port;
		$this->settings = array_merge($this->settings, $settings);
		$this->client = new \Swoole\Coroutine\Http2\Client($this->host, $this->port);
		return $this;
	}

	public function set(array $settings): self {
		$this->settings = array_merge($this->settings, $settings ?? []);
		return $this;
	}

	/**
	 * Esbalish a connection to the remote endpoint
	 */
	public function connect(): self {
		$this->client->set($this->settings);
		if (!$this->client->connect()) {
			throw new ClientException(swoole_strerror($this->client->errCode, 9) . " {$this->client->host}:{$this->client->port}", $this->client->errCode);
		}
		\Swoole\Coroutine::create(function () {
			while (!$this->closed && [$streamId, $data, $pipeline, $trailers] = $this->recvData()) {
				if ($streamId > 0 && !$pipeline) {
					$this->streams[$streamId][0]->push([$data, $trailers]);
					$this->streams[$streamId][0]->close();
					unset($this->streams[$streamId]);
				} elseif ($streamId > 0) {
					$this->streams[$streamId][0]->push([$data, $trailers]);
				}
			}
		});
		return $this;
	}

	/**
	 * Get the stats of the client
	 */
	public function stats(): array {
		return $this->client->stats();
	}

	/**
	 * Close the connection to the remote endpoint
	 */
	public function close() {
		$this->closed = true;
		$this->client->close();
	}

	/**
	 * Send message to remote endpoint, either end the stream or not depending on $mode of the client
	 * @param mixed $method
	 * @param mixed $message
	 * @param mixed $type
	 */
	public function send($method, $message, $type = 'proto') {
		$isEndStream = $this->mode === Constant::GRPC_CALL;
		$retry = 0;
		while ($retry++ < $this->settings['max_retries']) {
			$streamId = $this->sendMessage($method, $message, $type);
			if ($streamId && $streamId > 0) {
				$this->streams[$streamId] = [new \Swoole\Coroutine\Channel(1), $isEndStream];
				$this->forced_reconnect = false;
				return $streamId;
			}
			if ($this->client->errCode > 0) {
				if (in_array($this->client->errCode, [32, 111, 5001]) && $this->settings['force_reconnect'] && $retry < $this->settings['max_retries']) {
					if ($this->forced_reconnect) {
						\Swoole\Coroutine::sleep(0.01);
						continue;
					}
					$this->client->close();
					$this->client->connect();
					$this->forced_reconnect = true;
					\Swoole\Coroutine::sleep(0.1);
					continue;
				}
				$this->forced_reconnect = false;
				throw new ClientException(swoole_strerror($this->client->errCode, 9) . " {$this->client->host}:{$this->client->port}", $this->client->errCode);
			}
			\Swoole\Coroutine::sleep(0.01);
		}
		$this->forced_reconnect = false;
		return false;
	}

	/**
	 * Receive the data from a stream in the established connection based on streamId.
	 * @param mixed $streamId
	 * @param mixed $timeout
	 */
	public function recv($streamId, $timeout = -1) {
		if ($this->streams[$streamId] ?? false) {
			$data = $this->streams[$streamId][0]->pop($this->settings['receive_timeout'] ?? $timeout);
			if (!$data) {
				return [null, ['grpc-status' => '4', 'grpc-message' => 'DEADLINE_EXCEEDED']];
			}
			return $data;
		}
		throw new ClientException(swoole_strerror(86, 9) . " {$this->client->host}:{$this->client->port}", 86);
	}

	/**
	 * Push message to the remote endpoint, used in client side streaming mode.
	 * @param mixed $streamId
	 * @param mixed $message
	 * @param mixed $type
	 * @param bool $end
	 */
	public function push($streamId, $message, $type = 'proto', $end = false) {
		if ($type === 'proto') {
			$payload = $message->serializeToString();
		} elseif ($type === 'json') {
			$payload = $message;
		}
		$payload = pack('CN', 0, strlen($payload)) . $payload;
		return $this->client->write($streamId, $payload, $end);
	}

	private function sendMessage($method, $message, $type) {
		$request = new \Swoole\Http2\Request();
		$request->pipeline = false;
		$request->method = 'POST';
		$request->path = $method;
		$request->headers = [
			'user-agent' => 'grpc-openswoole/' . \SWOOLE_VERSION,
			'content-type' => 'application/grpc+' . $type,
			'te' => 'trailers',
		];
		if ($type === 'proto') {
			$payload = $message->serializeToString();
		} elseif ($type === 'json') {
			$payload = $message;
		}
		$request->data = pack('CN', 0, strlen($payload)) . $payload;
		return $this->client->send($request);
	}

	private function recvData() {
		if ($this->mode === Constant::GRPC_CALL) {
			$response = $this->client->recv($this->settings['timeout'] * $this->settings['max_retries']);
		} else {
			$response = $this->client->read($this->settings['timeout'] * $this->settings['max_retries']);
		}
		if (!$response) {
			if ($this->client->errCode > 0) {
				//throw new ClientException(swoole_strerror($this->client->errCode, 9) . " {$this->client->host}:{$this->client->port}", $this->client->errCode);
			}
			\Swoole\Coroutine::sleep(1);
			return [0, null, false, null];
		}
		if ($response) {
			if ($response->data) {
				$data = substr($response->data, 5);
			} else {
				$data = '';
			}
			$trailers = ['grpc-status' => $response->headers['grpc-status'] ?? '0', 'grpc-message' => $response->headers['grpc-message'] ?? ''];
			return [$response->streamId, $data, $response->pipeline, $trailers];
		}
		return [0, null, false, null];
	}

}
