<?php
/**
 * Kafka
 *
 * Filename: MKafka.class.php
 *
 * @author liyan
 * @since 2016 4 19
 */
require_once __DIR__.'/../util/Kafka/__kafka_init.php';

class MKafka {

    private static $producer;
    private static $consumer;
    protected static $arrMessages = array();

    public static function getProducer() {
        if(empty(MKafka::$producer)) {
            $zkConfig = ModuleKafka::config('zookeeper');
            MKafka::$producer = \Kafka\Produce::getInstance($zkConfig['host'], $zkConfig['timeout']);
        }
        return MKafka::$producer;
    }

    /**
     * @return  Kafka_SimpleConsumer
     */
    public static function getConsumer() {
        if(empty(MKafka::$consumer)) {
            $zkConfig = ModuleKafka::config('zookeeper');
            MKafka::$consumer = \Kafka\Consumer::getInstance($zkConfig['host'], $zkConfig['timeout']);
            // $maxbytes = Config::runtimeConfigForKeyPath('kafka.$.consumer.maxbytes');
            // MKafka::$consumer->setMaxBytes($maxbytes);
        }
        return MKafka::$consumer;
    }

    public static function sendAck() {
        $result = self::send(-1);
        return $result;
    }

    public static function sendWithoutAck() {
        return self::send(0);
    }

    /**
     * 发送
     *
     * @param string $topic
     * @return bool
     */
    public static function produce($topic, $message, $partId = 'anyone') {
        $message = MKafkaMessage::encodeKafkaMessage($message);
        self::$arrMessages[$topic][$partId][] = $message;
    }

    public static function send($ack = -1) {
        $producer = self::getProducer();

        foreach (self::$arrMessages as $topic => $topicMessages) {
            $partitions = $producer->getAvailablePartitions($topic);
            foreach ($topicMessages as $partId => $messages) {
                if ('anyone' === $partId) {
                    $shuffleParts = $partitions;
                    shuffle($shuffleParts);
                    $p = array_pop($shuffleParts);
                } else {
                    $p = $partitions[crc32($partId) % count($partitions)];
                }
                $producer->setMessages($topic, $p, $messages);
            }
        }

        return $producer->setRequireAck($ack)->send();
    }

    /**
     * 消费
     * @param  [type] $topic            [description]
     * @param  mix  $partitionOffsets   偏移量 - 1.int, topic的每个partition的offset都相同；2.array, 分别设定每个partition的offset
     * @param  string $group            [description]
     * @return [type]                   [description]
     */
    public static function consume($topic, $group = 'default_group', $offset = null) {
        $consumer = MKafka::getConsumer();

        if (is_null($offset)) {
            $offset = self::getTopicOffset($topic);
        }

        if ($group) {
            $consumer->setGroup($group);
        }

        $consumer->setFromOffset(true);
        if (is_numeric($offset)) {
            $consumer->setTopic($topic, $offset);
        } elseif (is_array($offset)) {
            foreach ($offset as $partId => $offset) {
                $consumer->setPartition($topic, $partId, $offset);
            }
        }
        $result = $consumer->fetch();
/*
        foreach ($result as $topicName => $partition) {
            foreach ($partition as $partId => $messageSet) {
                $host = $consumer->getClient()->getHostByPartition($topicName, $partId);
                printl($host, 'part', $partId);
                // var_dump($host);
            //var_dump($partition->getHighOffset());
                foreach ($messageSet as $message) {
                    printl($message);
                   // var_dump((string)$message);
                }
            //var_dump($partition->getMessageOffset());
            }
        }
        die();
//*/
        return MKafkaMessage::message($result, $consumer);
    }

    public static function getTopicPartitions($topic) {
        $producer = self::getProducer();
        $partitions = $producer->getAvailablePartitions($topic);
        return $partitions;
    }

    protected static function getTopicOffset($topic) {
        $topic_path = Config::runtimeConfigForKeyPath('kafka.offset_path');
        $partitions = MKafka::getTopicPartitions($topic);
        $offset = array();
        foreach ($partitions as $partId) {
            $partition_file = sprintf("%s/kafka-consume/%s/offset/%s", $topic_path, $topic, $partId);
            $offset[$partId] = 0;
            if (file_exists($partition_file)) {
                $offset[$partId] = file_get_contents($partition_file);
            }
        }
        return $offset;
    }

    public static function persistentTopicPartitionOffset($topic, $partId, $offset) {
        $topic_path = Config::runtimeConfigForKeyPath('kafka.offset_path');
        $partition_file = sprintf("%s/kafka-consume/%s/offset/%s", $topic_path, $topic, $partId);
        if (!is_dir(dirname($partition_file))) {
            mkdir(dirname($partition_file), 0777, true);
        }
        file_put_contents($partition_file, $offset + 1);
    }

    public static function persistentTopicOffset($topicOffset) {
        foreach ($topicOffset as $topic => $partitions) {
            foreach ($partitions as $partId => $offset) {
                self::persistentTopicPartitionOffset($topic, $partId, $offset);
            }
        }
    }

}
