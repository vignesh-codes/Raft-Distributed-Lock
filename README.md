# Locking Service

## Getting Started

Follow these steps to get the Locking Service application running on your machine.

### 1. Clone the Repository

Clone the repository from GitHub using the following command:

```bash
git clone https://github.com/FitrahHaque/Distributed-Lock
```

### 2. Navigate to the Project Directory

```bash
cd Raft-Consensus
```

### 3. Install Dependencies

```bash
go mod tidy
```

### 4. Run the Application

```bash
go run main.go
```

You will see a menu to choose actions to perform, including Data Store, Server, and Client operations.

## Demo

Follow the steps below to run a demonstration of the distributed locking mechanism.

### 1. Start the Data Store

Open a new terminal window within the `Raft-Consensus` directory and execute:

```bash
go run main.go

# Select Data Store (option D)
D
```

### 2. Launch Multiple Clients

Open 3 separate terminals and in each run:

```bash
go run main.go

# Select Client (option C)
C

# Assign each client a unique name:
1 client1
```

Repeat this for `client2` and `client3`.

### 3. Set Up a Cluster of 5 Servers

Open 5 terminals. In each, run:

```bash
go run main.go

# Select Server (option S)
S

# Provide a unique Server ID (0 to 4):
1 0
```

Then, on terminals for servers 1-4, connect to server 0 to form the cluster:

```bash
# Connect to serverID 0
13 0 [::]:8080
```

Check the current leader status:

```bash
9
```

### 4. Connect Clients to the Cluster

Run the following on each client terminal to connect to the cluster:

```bash
2 10
```

### 5. Request, Use, and Release a Lock

On `client1`, acquire lock `l1` for 60 seconds:

```bash
3 l1 60
```

After the lock is acquired, write data to the store:

```bash
5 hi l1
```

Verify the write operation by checking the file `client/data.txt`.

Finally, release the lock:

```bash
4 l1
```

Check server terminals to verify the lock release status.

### 6. Simultaneous Lock Requests

On `client2`, request lock `l1`:

```bash
3 l1 30
```

On `client3`, simultaneously request lock `l1`:

```bash
3 l1 30
```

`client2` will obtain the lock first. After 30 seconds, it will automatically pass to `client3`. Verify this by observing client and server logs.

### 7. Leader Crash Simulation

Request lock `l2` simultaneously from all three clients:

```bash
3 l2 40
```

Initially, `client1` acquires the lock. Now simulate a leader crash (assuming the leader is server 0):

```bash
# Press Control-C-Enter to crash the leader.
```

Clients will automatically reconnect to a newly elected leader. Verify that pending lock requests are processed correctly by checking logs across terminals.