package com.example.alfresco.events;

import jakarta.jms.Connection;
import jakarta.jms.ConnectionFactory;
import jakarta.jms.Destination;
import jakarta.jms.MessageProducer;
import jakarta.jms.Session;
import jakarta.jms.TextMessage;

import org.alfresco.service.cmr.repository.NodeRef;
import org.apache.activemq.ActiveMQConnectionFactory;

import java.util.HashMap;
import java.util.Map;

public class ActiveMQPublisher {

    private final String brokerUrl;
    private final String queueName;

    public ActiveMQPublisher(String brokerUrl, String queueName) {
        this.brokerUrl = brokerUrl;
        this.queueName = queueName;
    }

    public void publish(String eventType, NodeRef nodeRef) {
        Map<String, Object> payload = new HashMap<>();
        payload.put("eventType", eventType);
        payload.put("nodeRef", nodeRef.toString());

        send(payload.toString());
    }

    private void send(String body) {
        try {
            ConnectionFactory factory =
                    new ActiveMQConnectionFactory(brokerUrl);

            Connection connection = factory.createConnection();
            connection.start();

            Session session =
                    connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            Destination destination = session.createQueue(queueName);
            MessageProducer producer = session.createProducer(destination);

            TextMessage message = session.createTextMessage(body);
            producer.send(message);

            session.close();
            connection.close();

        } catch (Exception e) {
            throw new RuntimeException("Failed to send message to ActiveMQ", e);
        }
    }
}
