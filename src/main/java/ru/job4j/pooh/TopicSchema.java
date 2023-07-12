package ru.job4j.pooh;

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;

public class TopicSchema implements Schema {
    private final ConcurrentMap<String, ConcurrentMap<Receiver, BlockingQueue<String>>> data = new ConcurrentHashMap<>();
    private final Condition condition = new Condition();

    @Override
    public void addReceiver(Receiver receiver) {
        data.putIfAbsent(receiver.name(), new ConcurrentHashMap<>());
        data.get(receiver.name()).putIfAbsent(receiver, new LinkedBlockingQueue<>());
        condition.on();
    }

    @Override
    public void publish(Message message) {
        data.computeIfPresent(message.name(), (topic, receivers) -> {
            receivers.forEach((receiver, queue) -> queue.add(message.text()));
            return receivers;
        });
        condition.on();
    }

    @Override
    public void run() {
        while (!Thread.currentThread().isInterrupted()) {
            do {
                condition.off();
                for (ConcurrentMap<Receiver, BlockingQueue<String>> topic : data.values()) {
                    for (Map.Entry<Receiver, BlockingQueue<String>> receiverBlockingQueueEntry : topic.entrySet()) {
                        Receiver receiver = receiverBlockingQueueEntry.getKey();
                        BlockingQueue<String> queue = receiverBlockingQueueEntry.getValue();
                        String text = queue.poll();
                        while (text != null) {
                            receiver.receive(text);
                            text = queue.poll();
                        }
                    }
                }
            } while (condition.check());
            try {
                condition.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
