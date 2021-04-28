from flask import Flask

class Api(Flask):
    def set_worker(self, Worker):
        self.worker = Worker
