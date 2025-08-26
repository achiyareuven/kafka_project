from app_producer.data_loader import DataLoader
from app_producer.producer import Producer
import os



class Manager:
    _index_interesting = 10
    _index_not_interesting = 10

    def __init__(self):
        self.data = DataLoader()
        self.interesting_data = self.data.data_interesting
        self.not_interesting_data = self.data.data_not_interesting
        self.tofic_interesting = os.getenv("TOPIC_INTERESTED","interesting")
        self.tofic_not_interesting = os.getenv("TOPIC_NOT_INTERESTED","not_interesting")
        self.producer = Producer()

    def send_interesting(self):
        start = self._index_interesting - 10
        end = self._index_interesting
        sent = 0
        for i in range(start, end):
            if i >= len(self.interesting_data):
                break
            self.producer.publish_message(self.tofic_interesting, self.interesting_data[i])
            sent += 1
        self._index_interesting += sent
        return sent

    def send_not_interesting(self):
        start = self._index_not_interesting - 10
        end = self._index_not_interesting
        sent = 0
        for i in range(start, end):
            if i >= len(self.not_interesting_data):
                break
            self.producer.publish_message(self.tofic_not_interesting, self.not_interesting_data[i])
            sent += 1
        self._index_not_interesting += sent
        return sent














