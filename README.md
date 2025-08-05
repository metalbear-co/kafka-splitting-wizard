This Python script allows for checking whether the prepared Kafka credentials are sufficient for the mirrord Operator to run Kafka splitting.
The script uses kafkactl CLI tool to run a simple local simulation of Kafka splitting.
Before using it, please make sure that:
1. kafkactl is available in path
2. kafkactl is properly configured, and uses the correct context and credentials

## `split` subcommand

This subcommand runs a simple simulation of Kafka splitting.
By doing this, it verifies that Kafka credentials are sufficient for executing all operations required for Kafka splitting.

The simulation will require consuming messages from an existing Kafka topic. The messages are expected to be produced outside of this script.

### Arguments

1. `--topic`: name of an existing topic to split. In the real world scenario, this is the topic consumed by the application deployed in your cluster.
2. `--consumer-group`: consumer group to use when consuming messages from the existing topic.
3. `--header-name`: name of the message header to use when filtering messages.
4. `--header-value-regex`: regular expression to use when filtering messages.

See [mirrord docs](https://metalbear.co/mirrord/docs/using-mirrord/queue-splitting#setting-a-filter-for-a-mirrord-run) for more info on header-based filtering.
Please note, that while mirrord supports filtering based on multiple headers, this script supports only one.

### Flow

The script:
1. Fetches metadata of the existing topic specified in command line arguments.
2. Creates two temporary topics, with names starting with `mirrord-tmp`.
   In the real world scenario, the topic containing `-fallback-` would be consumed by the application deployed in your cluster.
   The other topic would be consumed by the local application of the mirrord user.
3. Consumes messages from the existing topic, and routes them to one of the temporary topics.
   The messages are consumed until Ctrl+C interrupt is received.
   At this point, you will need to externally produce messages to be routed to temporary topics.
4. Deletes temporary topics.

If the script exits with code 0, it means that your Kafka credentials are fully functional!

## `cleanup` subcommand

This subcommand deletes *all* temporary mirrord topics found in the Kafka cluster.
It will delete all Kafka topics which names start with `mirrord-tmp`.

## kafkactl and Python versions

This script was tested with:
1. kafkactl v5.11.1
2. Python v3.12.3
