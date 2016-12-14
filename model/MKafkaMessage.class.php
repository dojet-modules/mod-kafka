<?php
/**
 * description
 *
 * Filename: MKafkaMessage.class.php
 *
 * @author liyan
 * @since 2016 4 26
 */
class MKafkaMessage implements Iterator {

    private $fetchTopic;

    private $partition;
    private $messageSet;

    private $valid;
    private $offset = array();
    public static $consumer;

    function __construct(\Kafka\Protocol\Fetch\Topic $fetchTopic) {
        $this->fetchTopic = $fetchTopic;
    }

    public static function message(\Kafka\Protocol\Fetch\Topic $fetchTopic, $consumer) {
        self::$consumer = $consumer;
        return new MKafkaMessage($fetchTopic);
    }

    public static function encodeKafkaMessage($message) {
        return json_encode(array('i' => $message));
    }

    public static function decodeKafkaMessage(\Kafka\Protocol\Fetch\Message $kafkaMessage) {
        $data = json_decode($kafkaMessage->getMessage(), true);
        if (false === $data || !isset($data['i'])) {
            throw new Exception("illegal kafka message", 1);
        }
        $message = $data['i'];
        return $message;
    }

    function rewind() {
        // var_dump(__METHOD__);
        $this->fetchTopic->rewind();
        if (!$this->fetchTopic->valid()) {
            $this->valid = false;
            return;
        }
        $this->partition = $this->fetchTopic->current();
        $this->partition->rewind();
        if (!$this->partition->valid()) {
            $this->valid = false;
            return;
        }
        $this->messageSet = $this->partition->current();
        $this->messageSet->rewind();

        // $host = self::$consumer->getClient()->getHostByPartition($this->fetchTopic->key(), $this->partition->key());
        // printl('rewind', 'host', $host, 'partId ', $this->partition->key());

        $this->valid = $this->toAvailMessage();
    }

    function current() {
        // var_dump(__METHOD__);
        $topicName = $this->fetchTopic->key();
        $partId = $this->partition->key();
        $offset = $this->messageSet->messageOffset();

        $this->offset[$topicName][$partId] = $offset;

        MKafka::persistentTopicPartitionOffset($topicName, $partId, $offset);

        $message = $this->messageSet->current();

        $host = self::$consumer->getClient()->getHostByPartition($topicName, $partId);
        // printl($host, 'part', $partId, $message);
        $message = MKafkaMessage::decodeKafkaMessage($message);

        return $message;
    }

    function key() {
        return null;
    }

    function next() {
        // var_dump(__METHOD__);
        $this->messageSet->next();
        $this->valid = $this->toAvailMessage();
    }

    function valid() {
        // var_dump(__METHOD__);
        return $this->valid;
    }

    private function toAvailMessage() {
        // $host = self::$consumer->getClient()->getHostByPartition($this->fetchTopic->key(), $this->partition->key());
        // printl($host, $this->fetchTopic->key(), 'part', $this->partition->key());

        try {
            if ($this->messageSet->valid()) {
                // println('messageSet is valid');
                return true;
            }
        } catch (Exception $e) {
            // printl($e->getCode(), $e->getMessage());
            return false;
        }

        $this->partition->valid() && $this->partition->next();
        if ($this->partition->valid()) {
            // println('swith to next partition');
            $this->messageSet = $this->partition->current();
            $this->messageSet->rewind();
            return $this->toAvailMessage();
        }
        // println('END of partition');

        $this->fetchTopic->valid() && $this->fetchTopic->next();
        if ($this->fetchTopic->valid()) {
            // println('switch to next topic');
            $this->partition = $this->fetchTopic->current();
            $this->partition->rewind();
            if ($this->partition->valid()) {
                $this->messageSet = $this->partition->current();
                $this->messageSet->rewind();
            }
            return $this->toAvailMessage();
        }
        // println('END of topic');

        return false;
    }

    public function offset($topic = null, $partition = null) {
        if (is_null($topic)) {
            return $this->offset;
        } elseif (is_null($partition)) {
            return XPath::path($topic, $this->offset);
        }
        return XPath::path(sprintf("%s.%s", $topic, $partition), $this->offset);
    }

}
