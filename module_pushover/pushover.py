import os
import requests

class Pushover:
    def __init__(self, app_token, user_key, device = None):
        self.app_token = app_token
        self.user_key = user_key
        self.device = device
    
    def send(self, title, message): 
        data = {
            "token": self.app_token,
            "user": self.user_key,
            "title": title,
            "message": message
        }
        if self.device:
            data["device"] = self.device
        
        response = requests.post("https://api.pushover.net/1/messages.json", data=data).json()
        if response["status"] == 1:
            return True
        else:
            raise Exception(f"Pushover error: {response['errors']}")