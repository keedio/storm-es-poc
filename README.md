# StormTopology-AuditActiveLogins

## Description

StormPDU is a test storm topology for learning purpose. This topology connects a kafka Spout, passes messages to a bolt in order to reformat the message and finally insert messages in  Elastic Search.

It counts the User Logins and User Logouts from an audit.log lines, readed from K

## Storm topology description

The topology has the following Components:  
  KafkaSpout -> ParserBolt -> ElasticSearchBolt
  
### KafkaSpout: 
Connects to a Kafka topic and get the audit lines (previously inserted with flume)  
This spout is based on https://github.com/wurstmeister/storm-kafka-0.8-plus

### ParserBolt
Parse the message to extract all the information and genererate a proper json deleting nested objects.

### ElasticSearchBolt
Is the responsible to inserting messages on Elastic Search
This bolt is based on https://github.com/ptgoetz/storm-hbase
  
## Compilation
  
## Config topology
# MANDATORY PROPERTIES

  

