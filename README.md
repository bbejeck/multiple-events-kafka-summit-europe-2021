Table of Contents
=================

  * [This is the code repo containing examples from the "Multiple Events in one topic" presentation at the Kafka Summit-Europe 2021](#this-is-the-code-repo-containing-examples-from-the-multiple-events-in-one-topic-presentation-at-the-kafka-summit-europe-2021)
      * [1. Confluent prerequisites](#1-confluent-prerequisites)
      * [2. Setting up Kafka Brokers and Schema Registry](#2-setting-up-kafka-brokers-and-schema-registry)
      * [3. Configure the properties](#3-configure-the-properties)
      * [4. Configure the Schema Registry Plugin](#4-configure-the-schema-registry-plugin)
      * [5. Create topics](#5-create-topics)
      * [6. Register Schemas](#6-register-schemas)
      * [7. Produce different event types to single topic](#7-produce-different-event-types-to-single-topic)
      * [8. Consuming records from multi-event topics](#8-consuming-records-from-multi-event-topics)
      * [9. Kafka Streams sample application](#8-kafka-streams-sample-application)
      * [10. CLEAN UP](#9-clean-up)

Created by [gh-md-toc](https://github.com/ekalinin/github-markdown-toc)

#Multiple Events in one topic
### This is the code repo containing examples from the "Multiple Events in one topic" presentation at the Kafka Summit-Europe 2021


#### 1. Confluent prerequisites
We are going to run all the examples in Confluent! So before starting with the code, you'll need to make sure you have a 
few things set-up.

First, if you don't already have an account on [Confluent](https://confluent.cloud/) go ahead and set one up now.
Next you'll need to install the [Confluent CLI](https://docs.confluent.io/ccloud-cli/current/install.html).  With these
two steps out of the way, you're all set!

#### 2. Setting up Kafka Brokers and Schema Registry
For the Kafka broker and Schema Registry instances you're going to use the `ccloud-stack` utility to get everything up and running.
The open source library [ccloud_library.sh](https://github.com/confluentinc/examples/blob/latest/utils/ccloud_library.sh) has functions for interacting
with Confluent, including `ccloud-stack`.

Run this command to get `ccloud_library.sh`:
```commandline
 wget -O ccloud_library.sh https://raw.githubusercontent.com/confluentinc/examples/latest/utils/ccloud_library.sh
 source ./ccloud_library.sh
```

With that done, let's create the stack of Confluent resources:

```
CLUSTER_CLOUD=aws
CLUSTER_REGION=us-west-2
ccloud::create_ccloud_stack
```

NOTE: Make sure you destroy all resources when the workshop concludes.

The `create` command generates a local config file, `java-service-account-NNNNN.config` when it completes. The `NNNNN` represents the service account id.  
Let's take a quick look at the file:
```
cat stack-configs/java-service-account-*.config
```

You should see something like this:

```
# ENVIRONMENT ID: <ENVIRONMENT ID>
# SERVICE ACCOUNT ID: <SERVICE ACCOUNT ID>
# KAFKA CLUSTER ID: <KAFKA CLUSTER ID>
# SCHEMA REGISTRY CLUSTER ID: <SCHEMA REGISTRY CLUSTER ID>
# ------------------------------
ssl.endpoint.identification.algorithm=https
security.protocol=SASL_SSL
sasl.mechanism=PLAIN
bootstrap.servers=<BROKER ENDPOINT>
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="<API KEY>" password="<API SECRET>";
basic.auth.credentials.source=USER_INFO
schema.registry.basic.auth.user.info=<SR API KEY>:<SR API SECRET>
schema.registry.url=https://<SR ENDPOINT>
```

We'll use these properties for connecting the `KafkaProducer`, `KafkaConsumer`, and `Kafka Streams` applications to the 
brokers and Schema Registry on Confluent.

#### 3. Configure the properties

To get started running any code you'll need to configure the required properties. In the `src/main/resources` directory 
you'll find a file named `config.properties.orig`, creata a copy of that file and rename it to `config.properties`.  
The project configured Git to ignore the `config.properties`
file as it contains the key and password that the code will use to interact with the broker and Schema Registry on Confluent.  
So this file should _*never get checked in*_!

After making a copy and renaming the config file run this command:
```
cat stack-configs/java-service-account-*.config >> src/main/resources/config.properties
```
  
#### 4. Configure the Schema Registry Plugin

Next make a copy of `build.gradle.orig` and rename the copy to `build.gradle`, we do this as we use the Schema Registry username and 
password in the [SR Gradle Plugin](https://github.com/ImFlog/schema-registry-plugin) and we want to make sure we don't share any of this
information by accidentally checking in a file.

Now open the `build.gradle` file and find this section:
```groovy
schemaRegistry {
    // set the url to schema.registry.url property
    url = ''

    credentials {
        // username is the characters up to the ':' in the basic.auth.user.info property
        username = ''
        // password is everything after ':' in the basic.auth.user.info property
        password = ''
    }
    // Details left out for clarity
}
```

Follow the directions in the comments for setting up access to Schema Registry in Confluent.

#### 5. Create topics

Now you need to create topics for the examples to use.  Using the properties you've copied over in a previous step,
run this gradle command:
NOTE: If you don't have Gradle installed, run `gradle wrapper` first

```
./gradlew createTopics
```

You'll see some output on the console concluding with something like:
```
Created all topics successfully
```
Under the covers, the gradle command executes the main method of `io.confluent.developer.Topics`.  `Topics` is helper class
that uses the [Admin](https://kafka.apache.org/27/javadoc/org/apache/kafka/clients/admin/Admin.html) interface to create topics
on your brokers in Confluent.  You can log into Confluent now and inspect the topics through the UI now.   Keep the Confluent
UI open as you'll need it in the next step.

#### 6. Register Schemas
Next you'll need to register some schemas.  If you recall from the presentation, when you have an Avro schema where the 
top level element is a `union` you need to register the individual schemas in the union first.  
Then you'll register the container schema itself along with references to the schemas making up the union element.

Fortunately, the gradle SR plugin makes this easy for us.  Here's the configuration in the `build.gradle` file:
```groovy
register {
        subject('page-view', 'src/main/avro/page_view.avsc', 'AVRO')
        subject('purchase', 'src/main/avro/purchase.avsc', 'AVRO')
        subject('avro-events-value', 'src/main/avro/all_events.avsc', 'AVRO')
                .addReference("io.confluent.developer.avro.PageView", "page-view", 1)
                .addReference("io.confluent.developer.avro.Purchase", "purchase", 1)
    }
```

Now to register these schemas, run this in the command line:
```
./gradlew registerSchemasTask
```

This task runs quickly, and you should see some text followed by this result in the console:
```
Build Successful
```
You don't need to register schemas for the Protobuf example as you'll run the producer with `auto.commit=true` and Protobuf
will recursively register any proto files included in the main schema.  For the second Avro example, which uses a record to 
wrap the `union`, you've already registered the referenced schemas, and we can use auto-registration on the "wrapper" record.

Using the Confluent UI you opened in the previous step, you can view the uploaded schemas by clicking in the `Schema Registry`
tab and click on the individual schemas to inspect them.

#### 7. Produce different event types to single topic

Now let's produce some records to Confluent brokers.  The `io.confluent.developer.clients.DataProducer` class runs three 
`KafkaProducer` clients serially, producing records to the `avro-events`, `avro-wrapped-events`, and `proto-event` topics.
Each producer sends different event types to each topic in either Avro or Protobuf format.

To produce the records, run this command:
```
./gradlew runProducer
```
You should see a few statements as each producer sends records to the Confluent brokers

#### 8. Consuming records from multi-event topics

Next, let's take a look at the `io.confluent.developer.clients.MultiEventConsumer` class.  The `MultiEventConsumer` runs three
`KafkaConsumer` instances, serially, and prints some details about the consumed records to the console.  The point of this
example shows one possible approach to working with multiple event-types in a single topic.

To run the consumer execute:
```
./gradlew runConsumer
```
Then you should see some details about each record in the console

#### 9. Kafka Streams sample application
Last, but not least, we have a basic Kafka Streams application demonstrating one possible approach to 
handling multiple event types from a topic.

The Kafka Streams app, `io.confluent.developer.streams.MultiEventKafkaStreamsExample` 
uses a [ValueTransformerWithKey](https://kafka.apache.org/27/javadoc/org/apache/kafka/streams/kstream/ValueTransformerWithKey.html) to pull
out details from each record and build up new record type, a `CustomerInfo` object.

To run the streams application use this command:
```
./gradlew runKafkaStreamsExample
```

After a few seconds, you'll see some details on the console about the new `CustomerInfo` record created by extracting
fields from the different event types coming from the source topic.

#### 10. CLEAN UP

This concludes the demo from the presentation.  Please stick around and view the code and schema files. Play around and experiment
some to get a feel for working with schema references and multi-event topics.

_*Make sure you tear down the Confluent resources you created at the begging of this mini-tutorial by running:*_

```
ccloud::destroy_ccloud_stack $SERVICE_ACCOUNT_ID
```
Where the `$SERVICE_ACCOUNT_ID` is the number on the `java-service-account-NNNNNNN.config` file.

