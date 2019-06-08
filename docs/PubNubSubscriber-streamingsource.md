#### **Description**

Reads realtime events from PubNub cloud.

#### **Usage**

PubNub is a programmable network for developing realtime applications;
an evolution from three-tier architecture, purpose-built to handle all
the complexities of data streams. PubNub operates at the edge of the
network to handle and apply logic to real-time data, thereby minimizing
latency to 250 milliseconds or less worldwide, and guaranteeing reliability
and scalability.

PubNub source can be used for retrieving events from the channels in realtime.

### Basic

#### **Properties**

* **Reference Name**

  Name used to uniquely identify this sink for lineage, annotating metadata, etc.

* **Channels**

  Dataset the tables belongs to. A dataset is contained within a specific project.
Datasets are top-level containers that are used to organize and control access to tables and views.
If dataset does not exist, it will be created.

* **Subscriber Key**

  Google Cloud Storage bucket to store temporary data in.
It will be automatically created if it does not exist, but will not be automatically deleted.
Temporary data will be deleted after it is loaded into BigQuery. If it is not provided, a unique
bucket will be created and then deleted after the run finishes.

### Advanced

#### **Properties**

* **Secure**

  The name of the field that will be used to determine which table to write to.
Defaults to `tablename`.

* **Reconnection Policy**

  The name of the field that will be used to determine which table to write to.
Defaults to `tablename`.

* **Connection Timeout**

  The name of the field that will be used to determine which table to write to.
Defaults to `tablename`.

* **Max Reconnect Attempt**

  The name of the field that will be used to determine which table to write to.
Defaults to `tablename`.

* **Use Proxy**

  The name of the field that will be used to determine which table to write to.
Defaults to `tablename`.

* **Type of Proxy**

  The name of the field that will be used to determine which table to write to.
Defaults to `tablename`.

* **Proxy Hostname**

  The name of the field that will be used to determine which table to write to.
Defaults to `tablename`.

* **Proxy Port**

  The name of the field that will be used to determine which table to write to.
Defaults to `tablename`.

* **Authorization Key**

  The name of the field that will be used to determine which table to write to.
Defaults to `tablename`.
