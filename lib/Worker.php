<?php
namespace ResqueScheduler;

use Psr\Log\LoggerInterface;

/**
 * ResqueScheduler worker to handle scheduling of delayed tasks.
 *
 * @package		ResqueScheduler
 * @author		Chris Boulton <chris@bigcommerce.com>
 * @author		Siddhartha Goyal <siddgoyal77@gmail.com>
 * @copyright	(c) 2012 Chris Boulton
 * @license		http://www.opensource.org/licenses/mit-license.php
 */
class Worker
{
	/**
	 *
	 * @var LoggerInterface
	 */
	private $logger;

	/**
	 * @var int Interval to sleep for between checking schedules.
	 */
	protected $interval = 5;

	/**
	* The primary loop for a worker.
	*
	* Every $interval (seconds), the scheduled queue will be checked for jobs
	* that should be pushed to Resque.
	*
	* @param int $interval How often to check schedules.
	*/
	public function work($interval = null)
	{
		if ($interval !== null) {
			$this->interval = $interval;
		}

		$this->updateProcLine('starting');

		while (true) {
			$this->handleDelayedItems();
			$this->updateProcLine('waiting, current ts '.self::formatTimestamp());
			$this->sleep();
		}
	}

	/**
	 * Handle delayed items for the next scheduled timestamp.
	 *
	 * Searches for any items that are due to be scheduled in Resque
	 * and adds them to the appropriate job queue in Resque.
	 *
	 * @param DateTime|int $timestamp Search for any items up to this timestamp to schedule.
	 */
	public function handleDelayedItems($timestamp = null)
	{
		while (($timestamp = ResqueScheduler::nextDelayedTimestamp($timestamp)) !== false) {
			$this->updateProcLine('processing delayed items at '.self::formatTimestamp($timestamp));
			$this->enqueueDelayedItemsForTimestamp($timestamp);
		}
	}

	/**
	 * Schedule all of the delayed jobs for a given timestamp.
	 *
	 * Searches for all items for a given timestamp, pulls them off the list of
	 * delayed jobs and pushes them across to Resque.
	 *
	 * @param DateTime|int $timestamp Search for any items up to this timestamp to schedule.
	 */
	public function enqueueDelayedItemsForTimestamp($timestamp)
	{
		$item = null;
		while ($item = ResqueScheduler::nextItemForTimestamp($timestamp)) {
			$status = 'queueing ' . $item['class'] . ' in ' . $item['queue'];
			$this->logger->debug($status);
			$this->updateProcLine($status);

			\Resque_Event::trigger('beforeDelayedEnqueue', array(
				'queue' => $item['queue'],
				'class' => $item['class'],
				'args'  => $item['args'],
			));

			$payload = array($item['queue'], $item['class'], $item['args']);
			call_user_func_array('Resque::enqueue', $payload);
		}
	}

	/**
	 *
	 * @param LoggerInterface $logger
	 */
	public function setLogger($logger) {
		$this->logger = $logger;
	}

	/**
	 * Sleep for the defined interval.
	 */
	protected function sleep()
	{
		sleep($this->interval);
	}

	/**
	 * Update the status of the current worker process.
	 *
	 * On supported systems (with the PECL proctitle module installed), update
	 * the name of the currently running process to indicate the current state
	 * of a worker.
	 *
	 * @param string $status The updated process title.
	 */
	private function updateProcLine($status)
	{
		$processTitle = 'resque-scheduler-' . ResqueScheduler::VERSION . ': ' . $status;
		if(function_exists('cli_set_process_title')) {
			@cli_set_process_title($processTitle);
		}
		else if(function_exists('setproctitle')) {
			setproctitle($processTitle);
		}
	}

	private static function formatTimestamp($ts = null) {
		$time = new \DateTime();
		if (null != $ts)
			$time->setTimestamp($ts);
		return $time->format("Y-m-d H:i:s");
	}
}
