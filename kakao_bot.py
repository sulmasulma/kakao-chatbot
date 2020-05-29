#!/usr/bin/env python

import sys
sys.path.append("./libs")
import os
import requests
import base64
import json
import logging
from enum import Enum

DEFAULT_API_VERSION = 4.0

## messaging types: "RESPONSE", "UPDATE", "MESSAGE_TAG"

class NotificationType(Enum):
    regular = "REGULAR"
    silent_push = "SILENT_PUSH"
    no_push = "no_push"

class Bot:

    def __init__(self, access_token, **kwargs):

        self.access_token = access_token
        self.api_version = kwargs.get('api_version') or DEFAULT_API_VERSION
        self.graph_url = 'https://graph.facebook.com/v{0}'.format(self.api_version)

    @property
    def auth_args(self):
        if not hasattr(self, '_auth_args'):
            auth = {
                'access_token': self.access_token
            }
            self._auth_args = auth
        return self._auth_args

    # 디벨로퍼 페이지 문서에 따른 코드 구성
    # https://developers.facebook.com/docs/messenger-platform/send-messages
    def send_message(self, recipient_id, payload, notification_type, messaging_type, tag):

        payload['recipient'] = {
            'id': recipient_id
        }

        #payload['notification_type'] = notification_type
        payload['messaging_type'] = messaging_type

        if tag is not None:
            payload['tag'] = tag

        request_endpoint = '{0}/me/messages'.format(self.graph_url)

        response = requests.post(
            request_endpoint,
            params = self.auth_args, # 권한 관련 부분. access_token
            json = payload
        )

        logging.info(payload)
        return response.json()

    def send_text(self, recipient_id, text, notification_type = NotificationType.regular, messaging_type = 'RESPONSE', tag = None):

        return self.send_message(
            recipient_id,
            {
                "message": {
                    "text": text
                }
            },
            notification_type,
            messaging_type,
            tag
        )

    def send_quick_replies(self, recipient_id, text, quick_replies, notification_type = NotificationType.regular, messaging_type = 'RESPONSE', tag = None):
        # 다시 send_message 메소드를 참조함
        return self.send_message(
            recipient_id,
            {
                "message":{
                    "text": text,
                    "quick_replies": quick_replies
                }
            },
            notification_type,
            messaging_type,
            tag
        )

    def send_attachment(self, recipient_id, attachment_type, payload, notification_type = NotificationType.regular, messaging_type = 'RESPONSE', tag = None):

        return self.send_message(
            recipient_id,
            {
                "message": {
                    "attachment":{
                        "type": attachment_type,
                        "payload": payload
                    }
                }
            },
            notification_type,
            messaging_type,
            tag
        )

    def send_action(self, recipient_id, action, notification_type = NotificationType.regular, messaging_type = 'RESPONSE', tag = None):

        return self.send_message(
            recipient_id,
            {
                "sender_action": action
            },
            notification_type,
            messaging_type,
            tag
        )

    def whitelist_domain(self, domain_list, domain_action_type):

        payload = {
            "setting_type": "domain_whitelisting",
            "whitelisted_domains": domain_list,
            "domain_action_type": domain_action_type
        }

        request_endpoint = '{0}/me/thread_settings'.format(self.graph_url)

        response = requests.post(
            request_endpoint,
            params = self.auth_args,
            json = payload
        )

        return response.json()

    def set_greeting(self, template):

        request_endpoint = '{0}/me/thread_settings'.format(self.graph_url)

        response = requests.post(
            request_endpoint,
            params = self.auth_args,
            json = {
                "setting_type": "greeting",
                "greeting": {
                    "text": template
                }
            }
        )

        return response

    # 메신저를 켰을 때 무엇을 보여줄지
    def set_get_started(self, text):

        request_endpoint = '{0}/me/messenger_profile'.format(self.graph_url)

        response = requests.post(
            request_endpoint,
            params = self.auth_args,
            json = {
                "get_started":{
                    "payload": text
                }
            }
        )

        return response

    def get_get_started(self):

        request_endpoint = '{0}/me/messenger_profile?fields=get_started'.format(self.graph_url)

        response = requests.get(
            request_endpoint,
            params = self.auth_args
        )

        return response

    def get_messenger_profile(self, field):

        request_endpoint = '{0}/me/messenger_profile?fields={1}'.format(self.graph_url, field)

        response = requests.get(
            request_endpoint,
            params = self.auth_args
        )

        return response


    def upload_attachment(self, url):

        request_endpoint = '{0}/me/message_attachments'.format(self.graph_url)

        response = requests.post(
            request_endpoint,
            params = self.auth_args,
            json = {
                "message":{
                    "attachment":{
                        "type": "image",
                        "payload": {
                            "is_reusable": True,
                            "url": url
                        }
                    }
                }
            }
        )

        return response
