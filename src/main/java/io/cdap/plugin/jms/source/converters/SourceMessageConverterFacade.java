package io.cdap.plugin.jms.source.converters;

import com.google.common.base.Strings;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.plugin.jms.common.JMSMessageType;
import io.cdap.plugin.jms.source.JMSStreamingSourceConfig;

import javax.jms.BytesMessage;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.ObjectMessage;
import javax.jms.TextMessage;

/**
 * A facade class that provides a single method to convert JMS messages to structured records.
 */
public class SourceMessageConverterFacade {

  /**
   * Creates a {@link StructuredRecord} from a JMS message.
   *
   * @param message the incoming JMS message
   * @param config  the {@link JMSStreamingSourceConfig} with all user provided property values
   * @return the {@link StructuredRecord} built out of the JMS message fields
   * @throws JMSException in case the method fails to read fields from the JMS message
   * @throws IllegalArgumentException in case the user provides a non-supported message type
   */
  public static StructuredRecord toStructuredRecord(Message message, JMSStreamingSourceConfig config)
    throws JMSException, IllegalArgumentException {
    String messageType;
    if (!Strings.isNullOrEmpty(config.getMessageType())) {
      messageType = config.getMessageType();
    } else {
      throw new RuntimeException("Message type should not be null.");
    }

    if (message instanceof BytesMessage && messageType.equals(JMSMessageType.BYTES)) {
      return BytesMessageToRecordConverter.bytesMessageToRecord(message, config);
    }
    if (message instanceof MapMessage && messageType.equals(JMSMessageType.MAP)) {
      return MapMessageToRecordConverter.mapMessageToRecord(message, config);
    }
    if (message instanceof ObjectMessage && messageType.equals(JMSMessageType.OBJECT)) {
      return ObjectMessageToRecordConverter.objectMessageToRecord(message, config);
    }
    if (message instanceof Message && messageType.equals(JMSMessageType.MESSAGE)) {
      return ObjectMessageToRecordConverter.objectMessageToRecord(message, config);
    }
    if (message instanceof TextMessage && messageType.equals(JMSMessageType.TEXT)) {
      return TextMessageToRecordConverter.textMessageToRecord(message, config);
    } else {
      throw new IllegalArgumentException("Message type should be one of Message, Text, Bytes, Map, or Object");
    }
  }
}
