kafka-input-format
==================

**kafka-input-format**: A Hadoop input format specific for batch loading messages from Kafka, it has
several features:

1. Automatically record current consumed offset of Kafka message queue into Zookeeper, avoid
   duplication.
2. Automatically distribute tasks according to Kafka's broker-partition locality, avoid data
   transmission on the network.

---

This project is open sourced under Apache License Version 2.0.

