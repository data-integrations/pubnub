#### **Description**

Reads realtime messages from PubNub cloud.

#### **Usage**

PubNub is a programmable network for developing realtime applications;
an evolution from three-tier architecture, purpose-built to handle all
the complexities of data streams. PubNub operates at the edge of the
network to handle and apply logic to real-time data, thereby minimizing
latency to 250 milliseconds or less worldwide, and guaranteeing reliability
and scalability.

PubNub source can be used for retrieving events from the channels in realtime.

### Basic

* **Reference Name**

  Name used to uniquely identify this sink for lineage, annotating metadata, etc.

* **Channels**

  List of PubNub channels to subscribe for messages.

* **Subscriber Key**

  Subscribe Key provided by PubNub. Available from PubNub admin panel.

### Advanced

* **Secure**

  Sets whether the communication with PubNub Hub is using TLS.

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