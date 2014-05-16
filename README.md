correlation-identifier
====================
This is an implementation of pattern Correlation Identifier [1].
Algorithm:
1. Requestor:
   - send messages synchronously
   
2. Replier
   - main Thread: receive new messages and delay them
   - ReplySender Thread: send replies for messages where delay time has passed.
     * Observer pattern has been implemented here, Requestors are listening on the correlationId. 

3. Execution:
   - Execute CorrelationService.java that starts several Requestors and one Replier

[1] http://www.enterpriseintegrationpatterns.com/CorrelationIdentifier.html