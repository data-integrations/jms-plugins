/*
 * Copyright © 2021 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.plugin.jms.common;

import com.google.common.base.Strings;
import io.cdap.plugin.jms.sink.JMSBatchSinkConfig;
import io.cdap.plugin.jms.source.JMSStreamingSourceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.Topic;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;

/**
 * A facade class that encapsulates the necessary functionality to: get the initial context, resolve the connection
 * factory, establish connection to JMS, create a session, resolve the destination by a queue or topic name, create
 * producer, create consumer, set message listener to a consumer, and start connection. This class handles exceptions
 * for all the functionalities provided.
 */
public class JMSConnection {

  private static final Logger LOG = LoggerFactory.getLogger(JMSConnection.class);
  private final JMSConfig config;

  public JMSConnection(JMSConfig config) {
    this.config = config;
  }

  /**
   * Gets the initial context by offering 'jndiContextFactory', 'providerUrl', 'topic'/'queue' name, `jndiUsername`,
   * and `jndiPassword` config properties.
   *
   * @return the {@link InitialContext} from the given properties
   */
  public Context getContext() {
    Properties properties = new Properties();
    properties.put(Context.INITIAL_CONTEXT_FACTORY, config.getJndiContextFactory());
    properties.put(Context.PROVIDER_URL, config.getProviderUrl());

    if (config instanceof JMSBatchSinkConfig) {
      String destinationName = ((JMSBatchSinkConfig) config).getDestinationName();
      if (config.getType().equals(JMSDataStructure.TOPIC.getName())) {
        properties.put(String.format("topic.%s", destinationName), destinationName);
      } else {
        properties.put(String.format("queue.%s", destinationName), destinationName);
      }
    } else {
      String sourceName = ((JMSStreamingSourceConfig) config).getSourceName();
      if (config.getType().equals(JMSDataStructure.TOPIC.getName())) {
        properties.put(String.format("topic.%s", sourceName), sourceName);
      } else {
        properties.put(String.format("queue.%s", sourceName), sourceName);
      }
    }

    if (!(Strings.isNullOrEmpty(config.getJndiUsername()) && Strings.isNullOrEmpty(config.getJndiPassword()))) {
      properties.put(Context.SECURITY_PRINCIPAL, config.getJndiUsername());
      properties.put(Context.SECURITY_CREDENTIALS, config.getJndiPassword());
    }

    try {
      return new InitialContext(properties);
    } catch (NamingException e) {
      throw new RuntimeException("Failed to create initial context for provider URL " + config.getProviderUrl() +
                                   " with principal " + config.getJndiUsername(), e);
    }
  }

  /**
   * Gets a {@link ConnectionFactory} by offering the `connectionFactory` config property.
   *
   * @param context an initial context
   * @return a connection factory
   */
  public ConnectionFactory getConnectionFactory(Context context) {
    try {
      return (ConnectionFactory) context.lookup(config.getConnectionFactory());
    } catch (NamingException e) {
      throw new RuntimeException(String.format("Failed to resolve the connection factory for %s.",
                                               config.getConnectionFactory()), e);
    }
  }

  /**
   * Creates a {@link Connection} by offering `jmsUsername` and `jmsPassword`. If a source {@link Topic} is to be
   * consumed set `clientId` which is needed by the JMS broker to identify the durable subscriber.
   *
   * @param connectionFactory a given connection factory
   * @return a connection to the JMS broker
   */
  public Connection createConnection(ConnectionFactory connectionFactory) {
    Connection connection = null;
    try {
      connection = connectionFactory.createConnection(config.getJmsUsername(), config.getJmsPassword());
    } catch (JMSException e) {
      throw new RuntimeException(String.format("%s: %s", e.getErrorCode(), e.getMessage()));
    }
    // If subscribing to a source topic, create a durable subscriber
    if (config.getType().equals(JMSDataStructure.TOPIC.getName())) {
      try {
        if (config instanceof JMSStreamingSourceConfig) {
          String clientId = "client-id-" + ((JMSStreamingSourceConfig) config).getSourceName();
          connection.setClientID(clientId);
        }
      } catch (JMSException e) {
        throw new RuntimeException("Cannot set Client Id", e);
      }
    }
    return connection;
  }

  /**
   * Starts connection of this client to the JMS broker.
   *
   * @param connection a given connection
   */
  public void startConnection(Connection connection) {
    try {
      connection.start();
    } catch (JMSException e) {
      throw new RuntimeException(String.format("%s: %s", e.getErrorCode(), e.getMessage()));
    }
  }

  /**
   * Stops connection of this client from the JMS broker.
   *
   * @param connection a given connection
   */
  public void stopConnection(Connection connection) {
    try {
      connection.stop();
    } catch (JMSException e) {
      throw new RuntimeException(String.format("%s: %s", e.getErrorCode(), e.getMessage()));
    }
  }

  /**
   * Closes connection of this client from the JMS broker.
   *
   * @param connection a given connection
   */
  public void closeConnection(Connection connection) {
    try {
      connection.close();
    } catch (JMSException e) {
      throw new RuntimeException(String.format("%s: %s", e.getErrorCode(), e.getMessage()));
    }
  }

  /**
   * Creates a {@link Session} between this client and the JMS broker.
   *
   * @param connection a given session
   * @return a session to the JMS broker
   */
  public Session createSession(Connection connection) {
    try {
      return connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
    } catch (JMSException e) {
      throw new RuntimeException(String.format("%s: %s", e.getErrorCode(), e.getMessage()));
    }
  }

  /**
   * Closes the {@link Session} between this client and the JMS broker.
   *
   * @param session a given session
   */
  public void closeSession(Session session) {
    try {
      session.close();
    } catch (JMSException e) {
      throw new RuntimeException(String.format("%s: %s", e.getErrorCode(), e.getMessage()));
    }
  }

  /**
   * Gets the source {@link Topic}/{@link Queue} depending on the `type` config parameter. {@link Destination} is the
   * parent class of {@link Topic} and {@link Queue}.
   *
   * @param context a given context
   * @return a source topic/queue that this client is about to consume messages from
   */
  public Destination getSource(Context context) {
    String sourceName = ((JMSStreamingSourceConfig) config).getSourceName();
    if (config.getType().equals(JMSDataStructure.TOPIC.getName())) {
      try {
        return (Topic) context.lookup(sourceName);
      } catch (NamingException e) {
        throw new RuntimeException("Failed to resolve the topic " + sourceName, e);
      }
    } else {
      try {
        return (Queue) context.lookup(sourceName);
      } catch (NamingException e) {
        throw new RuntimeException("Failed to resolve the queue " + sourceName, e);
      }
    }
  }

  /**
   * Gets a sink {@link Topic}/{@link Queue} depending on the `type` config parameter. {@link Destination} is the
   * parent class of {@link Topic} and {@link Queue}. If no sink topic/queue name is provided, a sink topic/queue is
   * automatically created.
   *
   * @param context a given context
   * @param session a given session needed to create the topic/queue in case it does not exist
   * @return a sink topic/queue this client is about to produce messages to
   */
  public Destination getSink(Context context, Session session) {
    String destinationName = ((JMSBatchSinkConfig) config).getDestinationName();

    if (config.getType().equals(JMSDataStructure.TOPIC.getName())) {
      try {
        return (Topic) context.lookup(destinationName);
      } catch (NamingException e) {
        LOG.warn("Failed to resolve queue " + destinationName, e);
        return createSinkTopic(session);
      }
    } else {
      try {
        return (Queue) context.lookup(destinationName);
      } catch (NamingException e) {
        LOG.warn("Failed to resolve queue " + destinationName, e);
        return createSinkQueue(session);
      }
    }
  }

  /**
   * Creates a sink {@link Topic}
   *
   * @param session a given session
   * @return a created topic
   */
  private Destination createSinkTopic(Session session) {
    String destinationName = ((JMSBatchSinkConfig) config).getDestinationName();
    LOG.info("Creating topic " + destinationName);
    try {
      return session.createTopic(destinationName);
    } catch (JMSException e) {
      throw new RuntimeException(String.format("%s: %s", e.getErrorCode(), e.getMessage()));
    }
  }

  /**
   * Creates a sink {@link Queue}
   *
   * @param session a given session
   * @return a created queue
   */
  private Destination createSinkQueue(Session session) {
    String destinationName = ((JMSBatchSinkConfig) config).getDestinationName();
    LOG.info("Creating queue " + destinationName);
    try {
      return session.createQueue(destinationName);
    } catch (JMSException e) {
      throw new RuntimeException(String.format("%s: %s", e.getErrorCode(), e.getMessage()));
    }
  }

  /**
   * Creates a {@link MessageConsumer} that consumes messages from a defined source {@link Topic}/{@link Queue}. In case
   * of a topic-consumer, a durable subscriber is created. A durable subscriber makes the JMS broker keep the state of
   * the offset consumed. Hence this client can restart consuming messages from the last offset not read.
   *
   * @param session     a given session
   * @param destination a source topic/queue this client is about to consume messages from
   * @return a created message consumer
   */
  public MessageConsumer createConsumer(Session session, Destination destination) {
    MessageConsumer messageConsumer = null;
    try {
      if (destination instanceof Topic) {
        String clientId = "subscriber-id-" + ((JMSStreamingSourceConfig) config).getSourceName();
        messageConsumer = session.createDurableSubscriber((Topic) destination, clientId);
      } else {
        messageConsumer = session.createConsumer(destination);
      }
    } catch (JMSException e) {
      throw new RuntimeException(String.format("%s: %s", e.getErrorCode(), e.getMessage()));
    }
    return messageConsumer;
  }

  /**
   * Creates a {@link MessageProducer} that produces messages to a defined sink {@link Topic}/{@link Queue}.
   *
   * @param session     a given session
   * @param destination a sink topic/queue this client is about to produce messages to
   * @return a created message producer
   */
  public MessageProducer createProducer(Session session, Destination destination) {
    try {
      return session.createProducer(destination);
    } catch (JMSException e) {
      throw new RuntimeException(String.format("%s: %s", e.getErrorCode(), e.getMessage()));
    }
  }

  /**
   * Sets a {@link MessageListener} to the {@link MessageConsumer}. This message listener has a `onMessage()` method
   * that gets automatically triggered in case a new message is produced to the queue/topic while the pipeline is in
   * the RUNNING state.
   *
   * @param messageListener a given message listener
   * @param messageConsumer a given message consumer
   */
  public void setMessageListener(MessageListener messageListener, MessageConsumer messageConsumer) {
    try {
      messageConsumer.setMessageListener(messageListener);
    } catch (JMSException e) {
      throw new RuntimeException(String.format("%s: %s", e.getErrorCode(), e.getMessage()));
    }
  }
}
