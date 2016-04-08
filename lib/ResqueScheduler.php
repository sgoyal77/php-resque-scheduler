<?php
namespace ResqueScheduler;

/**
* ResqueScheduler core class to handle scheduling of jobs in the future.
*
* @package		ResqueScheduler
* @author		Chris Boulton <chris@bigcommerce.com>
* @author		Siddhartha Goyal <siddgoyal77@gmail.com>
* @copyright	(c) 2012 Chris Boulton
* @license		http://www.opensource.org/licenses/mit-license.php
*/
class ResqueScheduler
{
	const VERSION = "1.0";

	private static $KEY_SCHEDULE = 'scheduler:schedule';
	private static $KEY_TIME = 'resque:scheduler:time:';
	private static $KEY_JOB = 'resque:scheduler:job:';
	private static $KEY_ID = 'scheduler:id:';

	/**
	 * Enqueue a job in a given number of seconds from now.
	 *
	 * Identical to Resque::enqueue, however the first argument is the number
	 * of seconds before the job should be executed.
	 *
	 * @param int $in Number of seconds from now when the job should be executed
	 * @param string $queue The name of the queue to place the job in
	 * @param string $class The name of the class that contains the code to execute the job
	 * @param array $args Any optional arguments that should be passed when the job is executed
	 * @return string the id of the job placed in the delayed queue (NOTE: this id will be different
	 *         than the id that gets generated when the job is queued into resque)
	 */
	public static function enqueueIn($in, $queue, $class, array $args = array())
	{
		return self::enqueueAt(time() + $in, $queue, $class, $args);
	}

	/**
	 * Enqueue a job for execution at a given timestamp.
	 *
	 * Identical to Resque::enqueue, however the first argument is a timestamp
	 * (either UNIX timestamp in integer format or an instance of the DateTime
	 * class in PHP).
	 *
	 * @param DateTime|int $at Instance of PHP DateTime object or int of UNIX timestamp
	 * @param string $queue The name of the queue to place the job in
	 * @param string $class The name of the class that contains the code to execute the job
	 * @param array $args Any optional arguments that should be passed when the job is executed
	 * @return string the id of the job placed in the delayed queue (NOTE: this id will be different
	 *         than the id that gets generated when the job is queued into resque)
	 */
	public static function enqueueAt($at, $queue, $class, $args = array())
	{
		self::validateJob($class, $queue);

		$job = self::jobToHash($queue, $class, $args);
		$id = self::delayedPush($at, $job);

		\Resque_Event::trigger('afterSchedule', array(
			'at'    => $at,
			'queue' => $queue,
			'class' => $class,
			'args'  => $args,
			'id' => $id
		));

		return $id;
	}

	/**
	 * Directly append a job to the delayed queue schedule.
	 *
	 * @param DateTime|int $timestamp Timestamp job is scheduled to be run at
	 * @param array $job hashmap representing the job that will get queued
	 * @return string the id of job pushed onto the delayed queue
	 */
	public static function delayedPush($timestamp, $job)
	{
		$redis = \Resque::redis();
		$timestamp = self::getTimestamp($timestamp);
		$job = json_encode($job);
		$id = \Resque::generateJobId();

		//store the job in three different ways (so that we can access the job by different
		//methods and also enable deleting of the job through a multitude of ways without
		//a performance hit)
		//1) in a hash set of job ids and their corresponding parameters stored by timestamp
		//2) in a hash set of job ids and their execution time stored by the job itself
		//3) by the job id
		$timekey = self::timeKey($timestamp);
		$jobkey = self::jobKey($job);
		$idkey = self::idKey($id);

		//by timestamp
		$redis->hset($timekey, $id, $job);
		$redis->expireat($timekey, $timestamp + 300);

		//by job itself
		$redis->hset($jobkey, $id, $timestamp);

		//by the job's id
		$redis->set($idkey, $timekey."-".$jobkey);
		$redis->expireat($idkey, $timestamp + 300);

		//add the timestamp to the list timestamps to process
		$redis->zadd(self::$KEY_SCHEDULE, $timestamp, $timestamp);

		return $id;
	}

	/**
	 * Get the total number of jobs in the delayed schedule.
	 *
	 * @return int number of scheduled jobs
	 */
	public static function getDelayedQueueScheduleSize()
	{
		return (int)\Resque::redis()->zcard(self::$KEY_SCHEDULE);
	}

	/**
	 * Get the number of jobs for a given timestamp in the delayed schedule.
	 *
	 * @param DateTime|int $timestamp Timestamp
	 * @return int Number of scheduled jobs.
	 */
	public static function getDelayedTimestampSize($timestamp)
	{
		return \Resque::redis()->hlen(self::timeKey($timestamp));
	}

	/**
	 * Removes a particular job that has been placed in the delay queue by its id.
	 *
	 * @param string $id the id of the job to remove
	 * @return boolean true if the job was successfully removed, false otherwise
	 */
	public static function removeDelayedJobById($id)
	{
		$redis = \Resque::$redis;

		//get the time and job represented by the specific id
		//by id
		//the data stored is represented as follows: [TIMEKEY]:[JOBKEY]
		$timeandjob = $redis->get(self::idKey($id));
		$timeandjob = explode("-", $timeandjob);
		$timekey = $timeandjob[0];
		$jobkey = $timeandjob[1];

		//using its id delete the job from the timestamp hash and from the job hash
		$redis->hdel($timekey, $id);
		$redis->hdel($jobkey, $id);

		//clean up any lingering data for that timestamp and job
		self::cleanupTimestamp($timekey);
		self::cleanupJobs($jobkey);

		//return true if we are able to delete the id
		return 1 == $redis->del(self::idKey($id));
	}

    /**
     * Remove a set of delayed jobs from the queue. This will remove all jobs (regardless
     * of timestamp) that are identified by a specific queue, the processing class,
     * and arguments.
     *
     * @param string $queue the name of the queue to which the job will get added when queued
     * @param string $class the name of the class that will process the job when queued
     * @param array $args the additional arguments passed to the job when it is processed
     * @return int number of jobs that were removed
     */
    public static function removeDelayedJobs($queue, $class, $args)
    {
    	$destroyed = 0;
       	$redis = \Resque::redis();

       	//for the job identified in the arguments get all the job ids and their
       	//execution times, and do the following for each:
       	//1) for the particular time remove the job id from the set of ids to execute
       	//2) remove the job id
       	$jobKey = self::jobKey(self::jobToHash($queue, $class, $args));
       	$idmap = $redis->hgetall($jobKey);
       	foreach ($idmap as $id => $timestamp) {

       		$redis->hdel(self::timeKey($timestamp), $id);
       		$redis->del(self::idKey($id));

       		$destroyed++;
       	}

       	//finally delete the job key itself as all jobs associated with the job have been
       	//canceled
       	$redis->del($jobKey);

       	return $destroyed;
    }

    /**
     * Removes a delayed job (identified by its queue, its processing class, and arguments)
     * queued at a specific timestamp.
     *
     * @param DateTime|int $timestamp the time at which to remove the specified job
     * @param string $queue the name of the queue to which the job will get added when queued
     * @param string $class the name of the class that will process the job when queued
     * @param array $args the additional arguments passed to the job when it is processed
     * @return boolean true if the job was removed, false otherwise
     */
    public static function removeDelayedJobFromTimestamp($timestamp, $queue, $class, $args)
    {
    	$timestamp = self::getTimestamp($timestamp);

        $redis = \Resque::redis();

        //get the particular list of jobs queued for the item, and find the id of the one
        //that is queued for the timestamp that is passed in
        $jobkey = self::jobKey(self::jobToHash($queue, $class, $args));
        $idmap = $redis->hgetall($jobkey);
        foreach ($idmap as $id => $ts) {

        	//if the timestamp of the particular job id matches the timestamp passed in
        	//then delete the job from the timestamp hash and from the job hash and
        	//delete that job id
        	//cleanup the 'job' and 'time' hashes
        	if (intval($ts) == $timestamp) {

        		$timekey = self::timeKey($timestamp);
				$redis->hdel($timekey, $id);
				$redis->hdel($jobkey, $id);
				$redis->del(self::idKey($id));
				self::cleanupJobs($jobkey);
				self::cleanupTimestamp($timekey);

				return true;
        	}
        }

        return false;
    }

	/**
	 * Generate hashmap of all job properties to be saved in the scheduled queue.
	 *
	 * @param string $queue name of the queue the job will be placed on
	 * @param string $class name of the job class
	 * @param array $args array of job arguments
	 */

	private static function jobToHash($queue, $class, $args)
	{
		return array(
			'class' => $class,
			'args'  => $args,
			'queue' => $queue,
		);
	}

	/**
	 * Find the first timestamp in the delayed schedule before/including the timestamp.
	 *
	 * Will find and return the first timestamp upto and including the given
	 * timestamp. This is the heart of the ResqueScheduler that will make sure
	 * that any jobs scheduled for the past when the worker wasn't running are
	 * also queued up.
	 *
	 * @param DateTime|int $timestamp Instance of DateTime or UNIX timestamp.
	 *                                Defaults to now.
	 * @return int|false UNIX timestamp, or false if nothing to run.
	 */
	public static function nextDelayedTimestamp($at = null)
	{
		if ($at === null) {
			$at = time();
		}
		else {
			$at = self::getTimestamp($at);
		}

		$items = \Resque::redis()->zrangebyscore(self::$KEY_SCHEDULE, '-inf', $at, array('limit' => array(0, 1)));
		if (!empty($items)) {
			return $items[0];
		}

		return false;
	}

	/**
	 * Pop a job off the delayed queue for a given timestamp.
	 *
	 * @param DateTime|int $timestamp instance of DateTime or UNIX timestamp
	 * @return array|boolean a single job at the specified timestamp, or false if there are
	 *         no jobs left
	 */
	public static function nextItemForTimestamp($timestamp)
	{
		$redis = \Resque::$redis;

		//get the timekey for the timestamp and get all the job ids associated with that
		//timestamp
		$timekey = self::timeKey($timestamp);
		$jobids = $redis->hkeys($timekey);

		//if there are job ids in the list then get the actual job for the very first
		//job id in the list, and return that
		//We also delete the job from the timestamp and job hashes, the job itself,
		//and cleanup the 'job' and 'time' hashes in case they are empty
		if (!empty($jobids)) {

			$job = $redis->hget($timekey, $jobids[0]);
			$job = json_decode($job, true);

			$redis->hdel($timekey, $jobids[0]);
			$redis->hdel(self::jobKey($job), $jobids[0]);
			$redis->del(self::idKey($jobids[0]));
			self::cleanupJobs(self::jobKey($job));
			self::cleanupTimestamp($timekey);

			return $job;
		} else {
			self::cleanupTimestamp($timekey);
			return false;
		}
	}

	/**
	 * If a particular timestamp has no more jobs associated with it then this function will
	 * delete the 'time' key associated with that particular key. It will also remove
	 * the timestamps from the list of timestamps to process in the schedule.
	 *
	 * @param string $timekey The timestamp key to cleanup
	 */
	private static function cleanupTimestamp($timekey)
	{
		try {
			$redis = \Resque::redis();

			if ($redis->hlen($timekey) == 0) {
				$redis->del($timekey);
				$timestamp = str_replace(self::$KEY_TIME, "", $timekey);
				$redis->zrem(self::$KEY_SCHEDULE, $timestamp);
			}
		} catch (\Exception $e) {
			//ignorable error, as nothing bad happens if this function fails
			//at most a particular timestamp might be picked up again and processed
			//but at some point it should get cleared
		}
	}

	/**
	 * If there are no jobs left for a particular job key then this function will
	 * delete the 'item' key associated with that particular key.
	 *
	 * @param string $jobkey The job key to cleanup
	 */
	private static function cleanupJobs($jobkey)
	{
		try {
			$redis = \Resque::redis();

			if ($redis->hlen($jobkey) == 0) {
				$redis->del($jobkey);
			}
		} catch (\Exception $e) {
			//ignorable error, as nothing bad happens if this function fails
		}
	}

	/**
	 * Ensure that supplied job class/queue is valid.
	 *
	 * @param string $class Name of job class.
	 * @param string $queue Name of queue.
	 * @throws Resque_Exception
	 */
	private static function validateJob($class, $queue)
	{
		if (empty($class)) {
			throw new \Resque_Exception('Jobs must be given a class.');
		}
		else if (empty($queue)) {
			throw new \Resque_Exception('Jobs must be put in a queue.');
		}

		return true;
	}

	/**
	 * Convert a timestamp in some format in to a unix timestamp as an integer.
	 *
	 * @param DateTime|int $timestamp Instance of DateTime or UNIX timestamp.
	 * @return int Timestamp
	 * @throws ResqueScheduler_InvalidTimestampException
	 */
	private static function getTimestamp($timestamp)
	{
		if ($timestamp instanceof \DateTime) {
			$timestamp = $timestamp->getTimestamp();
		}

		if ((int)$timestamp != $timestamp) {
			throw new InvalidTimestampException(
					'The supplied timestamp value could not be converted to an integer.'
					);
		}

		return (int)$timestamp;
	}

	/**
	 *
	 * @param DateTime|int $timestamp A DateTime instance of UNIX timestamp
	 */
	private static function timeKey($timestamp) {
		if (is_int($timestamp))
			return self::$KEY_TIME . $timestamp;
		else
			return self::$KEY_TIME . self::getTimestamp($timestamp);
	}

	/**
	 *
	 * @param array $job the array of parameters that represent the job
	 */
	private static function jobKey($job) {
		if (is_array($job))
			return self::$KEY_JOB . md5(json_encode($job));
		else
			return self::$KEY_JOB . md5($job);
	}

	private static function idKey($id) {
		return self::$KEY_ID . $id;
	}
}
