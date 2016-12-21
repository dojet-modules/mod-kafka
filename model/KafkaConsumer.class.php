<?php
/**
 * kafka消费者基类
 *
 * @author liyan
 * @since 2016 4 25
 */
DAssert::assert(substr(php_sapi_name(), 0, 3) === 'cli', 'kafka consumer must run in cli mode');

Config::loadConfig(SCONFIG.'kafka_consume');

abstract class KafkaConsumer {

    abstract protected static function plugins();

    public static function start() {
        Daemon::run();
        $arrPlugins = static::plugins();
        foreach ($arrPlugins as $plugin) {
            require_once $plugin;
        }

        $arrTopics = MCliParam::param('--topic', '-t');

        while (1) {
            $count = 0;
            foreach ($arrTopics as $topic) {
                try {
                    $kafkaMessage = MKafka::consume($topic);
                } catch (Exception $e) {
                    Trace::warn('kafka consume exception: '.$e->getMessage(), __FILE__);
                    $kafkaMessage = MKafka::consume($topic);
                    //continue;
                }
                $count += self::traveMessage($topic, $kafkaMessage, 3);
            }

            // break;
            if ($count === 0) {
                // println('sleep 1');
                sleep(1);
            }
        }
    }

    protected static function traveMessage($topic, $kafkaMessage, $num = 1) {
        $count = 0;
        if (0 == $num) {
            return $count;
        }

        foreach ($kafkaMessage as $message) {
            // var_dump($message);
            NotificationCenter::postNotify('kafka_topic_consume', $topic, $message);
            if (++$count >= $num) {
                break;
            }
        }

        return $count;
    }

}

////////////////////////////////////

