# JMS Batch Sink


Description
-----------
Produces JMS messages of types as *Message*, *TextMessage*, *MapMessage*, *BytesMessage*, and *ObjectMessage* from a
given queue/topic. In case the queue/topic cannot get found with the specific name provided in the parameter 
*Destination Queue/Topic Name* a queue/topic will automatically get created.

JMS Batch Sink supports producing JMS messages

### Producing Headers 
JMS Batch Sink plugin does not support setting message headers. 

### Producing Properties
JMS Batch Sink plugin does not support setting message properties for *TextMessage*, *MapMessage*, *BytesMessage* and 
*ObjectMessage*, but it supports for *Message* type. 

### Producing JMS TextMessage
A JMS *TextMessage* accepts a single String text field in the body. When producing *TextMessage*'s, there are 
two ways of how the plugin generates them:

1. If the output schema consists of a single field of whatever primitive data type, a *TextMessage* is generated with
the value coming out of that field.
   
2. If the output schema consists of multiple fields, a stringified json object is created out of them and gets set to
the message body.
   
### Producing JMS MapMessage
A JMS *MapMessage* accepts a `key: value` set of fields. When producing *MapMessage*'s, from each field and data type
of the output schema, a field with the exact same name and data type will get loaded inside the *MapMessage* body.

### Producing JMS ObjectMessage
A JMS *ObjectMessage* a single serializable field. When producing *ObjectMessage*'s, the complete incoming record will 
get converted to a string json object and serialized. This serializable value then is loaded in the *ObjectMessage* 
body.

### Producing JMS BytesMessage
A JMS *BytesMessage* comes accepts a set of payloads with different data types. When producing *ByteMessage*'s, from 
each field and data type of the output schema, a field with name <data-type>_body (e.g., int_body, double_body, ... 
string_body) and data type will get loaded inside the *BytesMessage* body. 

### Producing JMS Message
A JMS *Message* message type comes only with *header* and *properties* but no *body*. When producing *Message*'s, from
each field and data type of the output schema, a field with the same name and data type will get loaded inside
the *Message* properties.

Use Case
--------
Use this JMS Sink plugin when you want to produce messages to a JMS Queue/Topic. 


Properties
----------
**Connection Factory**: Name of the connection factory. If not specified, the value *ConnectionFactory* is considered by
default.

**JMS Username**: Username to connect to JMS. This property is mandatory.

**JMS Password**: Password to connect to JMS. This property is mandatory.

**Provider URL**: Provider URL of the JMS Provider. This property is mandatory. For example for the *ActiveMQ* provider
you can set: `tcp://hostname:61616`.

**Type**: Queue or Topic. Queue is considered by default.

**Destination Queue or Topic Name**: The source queue/topic to consume messages from. The queue/topic must already by created.

**JNDI Context Factory**: Name of the context factory. This property is optional. For example for the *ActiveMQ*
provider you can set: `org.apache.activemq.jndi.ActiveMQInitialContextFactory`.

**JNDI Username**: Username for JNDI. This property is optional.

**JNDI Password**: Password for JNDI. This property is optional.

**Message Type**: The type of messages you intend to consume. A JMS message could be of the following types: *Message*,
*Text*, *Bytes* and *Map*, and *Object*. By default, *Text* message type is considered. The *payload* field of the
output schema gets switched to the appropriate data type upon the selection of a message type and *validate* button click.

Example
-------
Say that you have created a simple pipeline with a File Source Plugin that read message from a given json file and a 
JMS Sink Plugin that will sink messages to a topic named "customer-master-data". Since each json record consists
of multiple fields, you have selected the appropriate JMS message type which is *MapMessage*. 

Say that the below record is found in the json file. 

```json
{"Name":"John", "Surname":"Doe", "Age":25}
```

After the pipeline gets deployed and run, a *MapMessage* will get generated and produced in the *customer-master-data* 
topic. Accessing this topic, you can see a *MapMessage* with three fields, each one corresponding to the json field
from the data file.

| Field Name        | Field Value        |
| ----------------- | ------------------ |
| Name              | John               |
| Surname           | Doe                |
| Age               | 25                 |