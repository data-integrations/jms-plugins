# JMS Streaming Source

Description
-----------
Consumes JMS messages of types as *Message*, *TextMessage*, *MapMessage*, *BytesMessage*, and *ObjectMessage* from a 
given queue/topic. The given queue/topic should already be created.  

A JMS message is composed of *header*, *properties* and *body* parts. These three parts take place on every message type
except *Message*. The JMS *Message* type is destined to transfer only *header* and *properties* data but no *body*.

### Consuming the header
The header fields supported are: *messageId*, *messageTimestamp*, *correlationId*, *replyTo*, *destination*,
*deliveryNode*, *redelivered*, *type*, *expiration*, and *priority*. 

By default, *header* is consumed. You can change this by switching the parameter `Keep Message Header` to false.

You can decide to keep/remove any field inside the *header* record. Adding a new field which is not supported or 
changing their data types might cause validation errors.


### Consuming properties
By default, *properties* are consumed. You can change this by switching the parameter `Keep Message Properties` to false.

When consuming message *properties* you have two options to go with the schema:

1. Keep the by default provided schema

- The field *properties* is set to String data type.
- All message properties are included into a single string json object.

2. Set the schema manually

- The field *properties* must be set to Record data type.
- You can specify the properties fields you want to consume under the *properties* record field. The not specified 
  properties fields are not consumed.   
- The specified field names and their data types must exactly match the ones inside the given message.

### Consuming TextMessage
A JMS *TextMessage* comes with a single string field. When consuming Text messages, the *body* field in the schema must
be of String data type.

### Consuming MapMessage
A JMS *MapMessage* message comes with a `key: value` set of fields. The field *body* must either be of String or Record data 
type. When consuming JMS *Map* messages you have two options to go with the schema:

1. Keep the by default provided schema

- The field *body* is set to String. 
- All `key: value` fields from the *MapMessage* are included into a single string json object.

2. Set the schema manually

- The field *body* must be set to Record data type.
- You can specify the *MapMessage* fields you want to consume inside the body record field.
- The field names and data types specified under the *body* field should exactly match the ones inside the *MapMessage*.
  
### Consuming ObjectMessage
A JMS *ObjectMessage* message comes with a single object field. When selecting *ObjectMessage* type, the *body* field in the 
schema must be of Array of Bytes data type. 

### Consuming BytesMessage
A JMS *BytesMessage* message comes with set of payloads that could be of different data types. The field *body* must either
be of String or Record data type. When consuming JMS *BytesMessage*'s you have two options to go with the schema:

1. Keep the by default provided schema

- The field *body* is set to String.
- All payloads from the *BytesMessage* are included into a single string json object. Field names are specified as
`int_body`, `double_body`, `string_body` etc.

2. Set the schema manually

- The field *body* must be set to Record data type.
- You can specify the *BytesMessage* fields you want to consume inside the *body* record field.
- The field data types specified under the body field in the schema should exactly match the ones inside the
  *BytesMessage*.

### Consuming Message
A JMS *Message* message type comes only with *header* and *properties* but no *body*. Hence, it is mandatory for this 
type of message to either have the *Keep Message Header* and/or *Keep Message Properties* set to true. 


Use Case
--------
Use this JMS Source plugin when you want to consume messages from a JMS Queue/Topic and write them to a Table. 

Properties
----------
**Connection Factory**: Name of the connection factory. If not specified, the value *ConnectionFactory* is considered by
 default.

**JMS Username**: Username to connect to JMS. This property is mandatory.

**JMS Password**: Password to connect to JMS. This property is mandatory.

**Provider URL**: Provider URL of the JMS Provider. This property is mandatory. For example for the *ActiveMQ* provider
 you can set: `tcp://hostname:61616`.

**Type**: Queue or Topic. Queue is considered by default.

**Source Queue or Topic Name**: The source queue/topic to consume messages from. The queue/topic must already by created.

**JNDI Context Factory**: Name of the context factory. This property is optional. For example for the *ActiveMQ* 
provider you can set: `org.apache.activemq.jndi.ActiveMQInitialContextFactory`.

**JNDI Username**: Username for JNDI. This property is optional.

**JNDI Password**: Password for JNDI. This property is optional.

**Keep Message Headers**: If true, message headers are consumed. Otherwise, not.

**Keep Message Properties**: If true, message properties are consumed. Otherwise, not.

**Message Type**: The type of messages you intend to consume. A JMS message could be of the following types: *Message*,
*Text*, *Bytes* and *Map*, and *Object*. By default, *Text* message type is considered. The *payload* field of the 
output schema gets switched to the appropriate data type upon the selection of a message type and *validate* button click. 


Example
-------
Say that there is a JMS topic named "news-article-topic" created in an ActiveMQ provider consisting of newly scrapped 
article news. Say that your intention is to create a simple pipeline that will read these events 
and store them in a certain bucked in Google Storage for later analysis. This pipeline will have a JMS Streaming Source 
at the front and a GCS Sink Plugin at the back.

Since the events purely contain text data, you selected *TextMessage* type. Say that you are interested in keeping the
headers and not properties. You leave the "Keep Message Header" parameter to `true` and switch "Keep Message Properties" 
to `false`. 

When the pipeline runs messages start getting consumed. Let's say that the next message to get consumed is the below one.

```text
ActiveMQTextMessage {
  commandId=5,
  responseRequired=true,
  messageId=ID: Producer-50444-1608735228752-1: 1: 1: 1: 1,
  priority=4,
  ...
  destination=topic: topic://status,
  transactionId=null,
  expiration=0,
  timestamp=1608735228894,
  correlationId=null,
  replyTo=null,
  persistent=true,
  type=null,
  ...
  text="Sometimes, saving a species means treating one animal at a time. The veterinarians at The Wildlife Hospital..."
  messageTimestamp=1619096400
}

```

We see that this is an ActiveMQTextMessage (ie., implementation of TextMessage interface) which the source plugin receives
and will convert to it a proper record. Since we kept the headers and the message is a *TextMessage* the output record will 
be the follwing one:

```json
{
  "header": {
    "messageId": "Producer-50444-1608735228752-1: 1: 1: 1: 1",
    "messageTimestamp": 1608735228894,
    "destination": "topic://status",
    "priority": 4
  },
  "body": "Sometimes, saving a species means treating one animal at a time. The veterinarians at The Wildlife Hospital..."
}
```

