package pulsar

/*
Test1: receive and reproduce message
1. Receive some messages from topic1
2. resend the messages to topic2 with transactions
3. Ack the messages with transactions
4. Commit the transactions / Abort the transactions
5. Receive message form topic2 and con not receive messages from topic1
   / Can not receive message from topic2 and can receive messages from topic1
Test2:



*/
