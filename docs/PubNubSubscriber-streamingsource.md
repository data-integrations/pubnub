#### **Description**

Reads realtime messages from PubNub cloud.

#### **Usage**

PubNub source can be used for retrieving events from the channels in realtime.

Each message from PubNub channel or channel is translated into a record that contains
timetoken representing 17-digit (nano seconds) precision unix time in UTC is a
ORTT (Origination Time Token),payload representing the message retrieved,subscription id,
publisher id of message generator and the channel the message was retrieved from when messages
from various channels are grouped into a channel group.

Channels are how messages are sent and received. Clients that subscribe to a channel will
receive messages that are published to that channel. Channels are very lightweight and flexible.
Channels exist merely by using them.

The cipher key is used to encrypt & decrypt data that is sent to (and through) PubNub.
The secret key is used for message-signing (HMAC - Hash-based Message Authentication Code)
to sign the message. Do not use the secret key as the cipher key.

### Basic

* **Reference Name**

  Name used to uniquely identify this sink for lineage, annotating metadata, etc.

* **Channels**

  List of PubNub channels to subscribe for messages.

* **Subscriber Key**

  Subscribe Key provided by PubNub. Available from PubNub admin panel.

### Advanced

* **Secure**

  Sets whether the communication with PubNub Hub is using TLS (Formerly SSL).

* **Reconnection Policy**

  Reconnection policy which will be used if/when networking goes down. There are three reconnection policy
  available.

    * **None** - Indicates that no action will be taken when connection goes down.
    * **Linear** - Will attempt to reconnect every 3 seconds.
    * **Exponential** - Uses exponential backoff to reconnect. Min back off of 1 seconds to max of 32 seconds.


* **Connection Timeout**

  The maximum number of seconds which the client should wait for connection before timing out. Defaults of 5 seconds.

* **Max Reconnect Attempt**

  Number of times client will attempt to connect before giving up. Defaults to 5.

* **Subscribe Timeout**

  Specifies subscribe timeout. Defaults to 310 seconds.

* **Cipher Key**

  Cipher key to encrypt all communications between PubNub and client.

* **Use Proxy**

  Specifies whether a proxy should be used.

* **Type of Proxy**

  The type of proxy to be used DIRECT, SOCKS or HTTP. Valid when 'Use Proxy' is set to 'Yes'.

* **Proxy Hostname**

  Hostname of proxy server. Valid when 'Use Proxy' is set to 'Yes'.

* **Proxy Port**

  Port on the host used by proxy server. Valid when 'Use Proxy' is set to 'Yes'.

* **Proxy Username**

  Specifies Proxy Username. Valid when 'Use Proxy' is set to 'Yes'.

* **Proxy Password**

  Specifies Proxy Password. Valid when 'Use Proxy' is set to 'Yes'.