'''Broker'''
import json
from concurrent import futures
from socket import timeout
import time
import multiprocessing
import threading
import grpc
import src.protos.managerservice_pb2_grpc as m_pb2_grpc
import src.protos.managerservice_pb2 as m_pb2
import src.protos.brokerservice_pb2_grpc as b_pb2_grpc
import src.protos.brokerservice_pb2 as b_pb2
from src.broker.utils import raise_error, raise_success

# Raft
from src.pysyncobjm import SyncObj, replicated, SyncObjConf
from src.pysyncobjm.transport import TCPTransport
from src.pysyncobjm.poller import createPoller
from src.pysyncobjm.node import TCPNode


class ManagerConnection:
    """
    Client for gRPC functionality
    """

    def __init__(self, server_host, server_port, broker_host, broker_port, raft_port):

        # instantiate broker_id
        self.registered = False

        # instantiate a channel
        self.channel = grpc.insecure_channel(
            '{}:{}'.format(server_host, server_port))

        # bind the client and the server
        self.stub = m_pb2_grpc.ManagerServiceStub(self.channel)

        # broker server communication channel
        self.broker_channel = grpc.insecure_channel(
            '{}:{}'.format(broker_host, broker_port))

        # bind to broker server
        self.broker_stub = b_pb2_grpc.BrokerServiceStub(self.broker_channel)

        self.raft_port = raft_port

    def health_check(self):
        """
            unsets self.registered if manager is disconnected
        """
        disconnected = False
        while True:
            try:
                self.stub.HealthCheck(
                    m_pb2.HeartBeat(broker_id=0))
                if disconnected:
                    print('Manager connected.')
                break
            except grpc.RpcError as e:
                if not disconnected:
                    print('Manager disconnected, retrying...', e)
                    self.registered = False
                    disconnected = True

    def register_broker_if_required(self, host, port, token):
        """
            Checks if manager is down and if it is, then re-register once it is up
        """
        self.health_check()
        if not self.registered:
            print('registering...')
            status = self.stub.RegisterBroker(m_pb2.BrokerDetails(
                host=host, port=port, token=token, raft_port=self.raft_port
            ))

            if status.status:
                self.registered = True
                print('Successfully registered.')
            else:
                print('registration failed.')


class Raft(SyncObj):
    """
        Each topic's partition is handled by a single Raft Instance
    """

    def __init__(self, transport, topic_partition, selfNodeAddr, otherNodeAddrs, conf):
        super(Raft, self).__init__(topic_partition, selfNodeAddr,
                                   otherNodeAddrs, conf, transport=transport)

        # init queries list, aka., topic's partition
        self.queries = []

        print('Created Raft Instance for:', topic_partition)
        self.topic_partition = topic_partition

    @replicated
    def append_query(self, query):
        # print('append_query executed for', self.topic_partition)
        self.queries.append(query)

    @replicated
    def remove_queries(self, n):
        for _ in range(n):
            self.queries.pop(0)

    def get_queries(self):
        return self.queries.copy()


class BrokerService(b_pb2_grpc.BrokerServiceServicer):

    def __init__(self, raft_port, other_raft_ports):
        super().__init__()
        self.__topics = {}
        self.__producers = {}
        self.broker_id = None
        self.__publish_lock = threading.Lock()

        # all raft communications happen through this port
        self.__raft_port = raft_port

        # raft ports of all other brokers
        self.__other_raft_ports = other_raft_ports

        # create TCPNodes
        self.__selfnode = TCPNode('localhost:' + self.__raft_port)
        self.__othernodes = []
        self.__portToTCPNode = {}
        for p in self.__other_raft_ports:
            if p != self.__raft_port:
                tcpnode = TCPNode('localhost:' + p)
                self.__othernodes.append(tcpnode)
                self.__portToTCPNode[p] = tcpnode

        # create common objects
        self.__conf = SyncObjConf(autoTick=False)
        self.__poller = createPoller(self.__conf.pollerType)

        # create transport object
        self.__transport = TCPTransport(
            self.__poller, self.__conf, self.__selfnode, self.__othernodes)

        # map to store raft instance for each topic's partition
        # self.__topic_partition_to_raft[(topic, partition)] = Raft(...)
        self.__topic_partition_to_raft = {}
        self.__topic_partitions = []

        # poll for messages
        threading.Thread(target=self.poll_thread).start()

    def poll_thread(self):
        while True:
            # select command is used internally and callbacks are attached
            self.__poller.poll(0.0)

            # tick the raft instances
            topic_partitions = self.__topic_partitions[:]
            for topic_partition in topic_partitions:
                self.__topic_partition_to_raft[topic_partition].doTick()

    def clear_data(self):
        self.__topic_partitions.clear()
        self.__topic_partition_to_raft.clear()
        for topic in self.__topics:
            for partition in self.__topics[topic]:
                if partition == 'consumers' or partition == 'producers':
                    continue
                self.__topics[topic][partition]["messages"].clear()

    def GetUpdates(self, request, context):
        # get the queries from the partition
        topic_partition = (request.topic, request.partition)
        # check if there is any update
        temp = self.__topic_partition_to_raft[topic_partition].get_queries()
        if (len(temp) > 0):
            self.__topic_partition_to_raft[topic_partition].remove_queries(len(temp), sync=True)
            for query in temp:
                yield b_pb2.Query(query=query)

    def SendTransaction(self, transaction_req, context):
        transaction = json.loads(transaction_req.data)
        response = self.process_transaction(transaction)
        return b_pb2.Response(data=bytes(json.dumps(response).encode('utf-8')))

    def process_transaction(self, transaction):
        req_type = transaction['req']
        if req_type == 'Enqueue' or req_type == 'EnqueueWithPartition':
            res = self.publish_message(
                transaction["producer_id"], transaction["topic"], transaction["message"])
            return res
        elif req_type == 'CreateTopic':
            topic = transaction['topic']
            if topic not in self.__topics:
                self.__topics[topic] = {str(self.broker_id): {"messages": []}}
            return {}
        elif req_type == 'ProducerRegister':
            topic = transaction['topic']
            producer_id = transaction['producer_id']
            if topic not in self.__topics:
                self.__topics[topic] = {str(self.broker_id): {"messages": []}}
            if str(producer_id) not in self.__producers:
                self.__producers[str(producer_id)] = {"topic": topic}
            return {}
        elif req_type == 'Init':
            self.broker_id = transaction['broker_id']
            print('connected to manager, broker id:', self.broker_id)
            self.clear_data()
            self.__topics = transaction['topics']
            self.__producers = transaction['producers']
            return {}
        elif req_type == 'ReplicaHandle':
            topic_partitions = transaction['topic_partitions']
            raftports = transaction['other_raftports']

            # create Raft Instances
            for i, topic_partition in enumerate(topic_partitions):
                raftothernodes = []
                for port in raftports[i]:
                    raftothernodes.append(self.__portToTCPNode[port])

                topic_partition = tuple(topic_partition)
                if topic_partition in self.__topic_partition_to_raft:
                    continue
                self.__topic_partition_to_raft[topic_partition] = Raft(
                    self.__transport, topic_partition, self.__selfnode, raftothernodes, self.__conf)
                self.__topic_partitions.append(topic_partition)

            return {}
        else:
            # return self.add_producer(transaction["pid"], transaction["topic"])
            return raise_error("Invalid transaction request.")

    def add_producer(self, producer_id: int, topic_name: str):
        self.__producers[producer_id]["topic"] = topic_name

    def publish_message(self, producer_id: int, topic_name: str, message: str):
        isLockAvailable = self.__publish_lock.acquire(blocking=False)
        if isLockAvailable is False:
            return raise_error("Lock cannot be acquired.")
        if topic_name not in self.__topics:
            self.__publish_lock.release()
            return raise_error("Topic " + topic_name + " doesn't exist.")
        if str(producer_id) not in self.__producers:
            self.__publish_lock.release()
            return raise_error("Producer doesn't exist.")
        if "topic" not in self.__producers[str(producer_id)] or self.__producers[str(producer_id)]["topic"] != topic_name:
            self.__publish_lock.release()
            return raise_error("Producer cannot publish to " + topic_name + ".")
        if str(self.broker_id) in self.__topics[topic_name]:
            self.__topics[topic_name][str(self.broker_id)]["messages"].append({
                "message": message,
                "subscribers": 0
            })

        else:
            self.__topics[topic_name][str(self.broker_id)] = {
                "messages": [{
                    "message": message,
                    "subscribers": 0
                }]
            }

        topic_partition = (topic_name, str(self.broker_id))
        if topic_partition not in self.__topic_partition_to_raft:
            self.__publish_lock.release()
            return raise_error("Raft Instance not ready.")

        query = "INSERT INTO topic(topic_name, partition_id,bias) SELECT '" + topic_name + "','" + str(self.broker_id) + \
                "', '0' WHERE NOT EXISTS (SELECT topic_name, partition_id FROM topic WHERE topic_name = '" + topic_name + \
                "' and partition_id =" + str(self.broker_id) + ");"
        # Raft append_query
        self.__topic_partition_to_raft[(
            topic_name, str(self.broker_id))].append_query(query, sync=True)

        query = "INSERT INTO message(message, topic_name, partition_id, subscribers) VALUES('" + \
                message + "', '" + topic_name + "', " + \
            str(self.broker_id) + ", " + str(0) + ");"
        # Raft append_query
        self.__topic_partition_to_raft[(
            topic_name, str(self.broker_id))].append_query(query, sync=True)

        res = raise_success("Message added successfully.")
        self.__publish_lock.release()
        return res


class Broker:
    def __init__(self, port, raft_port, other_raft_ports):
        # retrieve broker config
        with open('./src/broker/broker.json', 'r') as config_file:
            self.config = json.load(config_file)
            self.host = self.config['host']
            self.port = port
            self.token = self.config['token']
        self.raft_port = raft_port

        # start broker service
        t = threading.Thread(target=self.serve, args=[
                             raft_port, other_raft_ports])
        t.start()

        # manager connection
        client = ManagerConnection(
            self.config['server_host'], self.config['server_port'],
            self.config['host'], self.port, self.raft_port
        )

        time.sleep(1)
        while True:
            client.register_broker_if_required(
                self.host, self.port, self.token)
            time.sleep(1)

    def serve(self, raft_port, other_raft_ports):
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=2))
        b_pb2_grpc.add_BrokerServiceServicer_to_server(
            BrokerService(raft_port, other_raft_ports), server)
        ip = '{}:{}'.format(self.config['host'], self.port)
        server.add_insecure_port('[::]:' + self.port)
        print('broker listening at:', ip)
        server.start()
        server.wait_for_termination()
