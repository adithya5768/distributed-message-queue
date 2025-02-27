# program for the prodcer to Register,Enqueue and Create topic
import requests

HOST = "http://127.0.0.1:"
PORT = "8001"


class MyProducerError(Exception):
    '''Producer has got Error'''


class MyProducer():

    def __init__(self):
        self.__topics = {}

    def RegisterProducer(self, p_topic):
        if p_topic in self.__topics:
            raise MyProducerError("Already registered for this topic.")
        # This function is used to Register producer by giving topicname and receiving a producer id from the server
        API_ENDPOINT = "/producer/register"
        url = HOST+PORT+API_ENDPOINT
        # HTTP link is used to connect to the server

        send_data = {'topic': p_topic}

        r = requests.post(url, json=send_data)

        received_data = r.json()
        try_count = 30
        # recieve the json file from the server
        # receive_data['status'] incidates the status whether it is success or failure
        while try_count:
            if r.status_code == 400 and "Lock cannot be acquired" in r.text:
                r = requests.post(url, json=send_data)
                received_data = r.json()
            else:
                break
            try_count -= 1
        else:
            raise MyProducerError(
                "Server is busy so please try after some time")
        if r.status_code == 200:
            self.__topics[p_topic] = received_data['producer_id']
        elif (r.status_code) == 400:
            raise MyProducerError("Error Occured :"+received_data['message'])

    def ListTopics(self):
        '''Returns the list of all the topics available the the produces have created'''
        API_ENDPOINT = "/topics"
        url = HOST+PORT+API_ENDPOINT

        r = requests.get(url)
        received_data = r.json()

        try_count = 30
        # recieve the json file from the server
        # receive_data['status'] incidates the status whether it is success or failure
        while try_count:
            if r.status_code == 400 and "Lock cannot be acquired" in r.text:
                r = requests.get(url)
                received_data = r.json()
            else:
                break
            try_count -= 1
        else:
            raise MyProducerError(
                "Server is busy so please try after some time")
        if r.status_code == 400:
            raise MyProducerError(received_data['message'])
        if r.status_code == 200:
            topicsList = received_data['topics']
            return list(topicsList)

    def Enqueue(self, p_topic, p_message, p_partition = 0):
        if p_topic not in self.__topics:
            raise MyProducerError("Cannot publish to this topic.")
        # This Function is used to Enqueue the created log message in the distributed queue
        API_ENDPOINT = "/producer/produce"
        url = HOST+PORT+API_ENDPOINT
        # HTTP link is used to connect to the server

        if p_partition != 0:
            PARAMS = {'topic': p_topic,
                  'producer_id': self.__topics[p_topic], 'message': p_message, 'partition': p_partition}
        else:
            PARAMS = {'topic': p_topic,
                  'producer_id': self.__topics[p_topic], 'message': p_message}
        # The above 3 parameters indicated topicname,producer-id and log_message created by the producer.

        r = requests.post(url, json=PARAMS)
        received_data = r.json()
        # recieve the json file from the server
        # receive_data['status'] incidates the status whether it is success or failure
        try_count = 30
        while try_count:
            if r.status_code == 400 and "Lock cannot be acquired" in r.text:
                r = requests.post(url, json=PARAMS)
                received_data = r.json()
            else:
                break
            try_count -= 1
        else:
            raise MyProducerError(
                "Server is busy so please try after some time")
        if (r.status_code == 200):
            return 0
        else:
            raise MyProducerError("Error Occured :" + received_data['message'])

    def CreateTopic(self, p_topic):
        # This Function is used to create topic name send by the producer
        API_ENDPOINT = "/topics"
        url = HOST+PORT+API_ENDPOINT
        # HTTP link is used to connect to the server
        PARAMS = {'name': p_topic}
        # Parameters passed is topic name
        r = requests.get(url, params=PARAMS)
        received_data = r.json()
        # receive_data['status'] incidates the status whether it is success or failure
        try_count = 30
        while try_count:
            if r.status_code == 400 and "Lock cannot be acquired" in r.text:
                r = requests.get(url, params=PARAMS)
                received_data = r.json()
            else:
                break
            try_count -= 1
        else:
            raise MyProducerError(
                "Server is busy so please try after some time")
        if (received_data['status'] == "success"):
            return 0
        else:
            raise MyProducerError("Error Occured :" + received_data['message'])


# print(p.RegisterProducer("example_topic"))
# print(p.Enqueue("example_topic",3,"post_created"))
# print(p.ListTopics())
# p.CreateTopic("example_topic")
# for i in range(5):
    # p.enqueue("example_topic",1,"post_created" + 'test' + str(i) )
# p.enqueue("example_topic",1,"Helloooo...")
