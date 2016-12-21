<?php
/**
 * kafka消费者插件基类
 *
 * @author liyan
 * @since 2014 4 25
 */
abstract class BaseKafkaConsumerPlugin extends BasePlugin {

    protected function bindActions() {
        return array(
            'kafka_topic_consume' => 'consume',
            );
    }

    /**
     * 插件处理的kafka主题
     * @return string
     */
    abstract protected function topic();

    final public function consume($topic, $message) {
        if ($topic !== $this->topic()) {
            return ;
        }
        return $this->work($message);
    }

    /**
     * 插件工作方法
     * @param  string $message
     */
    abstract protected function work($message);

}
