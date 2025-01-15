import datetime
from zoneinfo import ZoneInfo
import json
import logging
import time
import os
from googleapiclient.discovery import build
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.errors import HttpError


class YouTubeStreamManager:
    SCOPES = ["https://www.googleapis.com/auth/youtube"]
    TOKEN_FILE = "token.secret"
    LOG_FILE = "yt-stream-manager.log"

    CONF_LOGGER = "logger"

    CONF_EMAIL = "email"
    CONF_ENABLE_EMAIL = "enable_email"
    CONF_SMTP_SERVER = "smtp_server"
    CONF_SMTP_PORT = "smtp_port"
    CONF_SENDER_EMAIL = "sender_email"
    CONF_SENDER_PASSWORD = "sender_password"
    CONF_RECIPIENT_EMAIL = "recipient_email"
    CONF_SUBJECT = "subject"

    CONF_STREAM_SETTINGS = "stream_settings"
    CONF_STREAM_ID = "stream_id"
    CONF_BROADCAST_ID = "broadcast_id"  # set by the system to later stop the broadcast
    CONF_TITLE = "title"
    CONF_DESCRIPTION = "description"
    CONF_PRIVACY = "privacy"
    CONF_TAGS = "tags"
    CONF_CATEGORY = "category"

    CONF_YOUTUBE_SETTINGS = "youtube_settings"
    CONF_CREDENTIALS_FILE = "credentials_file"

    CONF_INFO = "info"
    CONF_DEBUG = "debug"

    LOGGER_OPTIONS = [CONF_INFO, CONF_DEBUG]

    CONF_PRIVATE = "private"
    CONF_PUBLIC = "public"

    CONF_PRIVACY_OPTIONS = [CONF_PRIVATE, CONF_PUBLIC]

    DEFAULT_CONFIG = {
        CONF_LOGGER: "info",
        CONF_EMAIL: {
            CONF_ENABLE_EMAIL: False,
            # CONF_SMTP_SERVER: "",
            # CONF_SMTP_PORT: 587,
            # CONF_SENDER_EMAIL: "",
            # CONF_SENDER_PASSWORD: "",
            # CONF_RECIPIENT_EMAIL: "",
            # CONF_SUBJECT: "",
        },
        CONF_STREAM_SETTINGS: {
            CONF_STREAM_ID: None,  # required if calling start_broadcast
            CONF_TITLE: "Live vom Bienenbaum",
            CONF_DESCRIPTION: "",
            CONF_PRIVACY: CONF_PRIVATE,
            CONF_TAGS: [],
            CONF_CATEGORY: 1,
        },
        CONF_YOUTUBE_SETTINGS: {
            CONF_CREDENTIALS_FILE: None
        },  # required if calling stop_broadcast
    }

    def __init__(self, config_file="config.json"):
        self.config_file = config_file
        self.config = self.DEFAULT_CONFIG
        self.logger = self._setup_logger()
        self.config = self._load_config()
        # might has changed after loading the config
        self._reload_log_level()
        self.logger.debug(f"Config:\n{self.config}")
        if not self._check_config():
            self.logger.error("Config failed")
            exit()

    def start_broadcast(self):
        # check if stream_id is set
        if self.config[self.CONF_STREAM_SETTINGS][self.CONF_STREAM_ID] is None:
            self.logger.error(
                f"Failed to start the Broadcast! It seems like no `{self.CONF_STREAM_ID}`is provided in the config under `{self.CONF_STREAM_SETTINGS}`- run the program with `create_stream` to get a `{self.CONF_STREAM_ID}`"
            )
            return None

        if self._authenticate() is None:
            self.logger.info("Authentication failed")
            return
        self.logger.info("Authentication was successfull")

        self.logger.info("Creating Broadcast..")
        if self._create_live_broadcast() is None:
            self.logger.info("Creating broadcast failed")
            return
        self.logger.info(
            f"Boradcast with the ID: `{self.broadcast_id}`was successfully created"
        )

        # store the broadcast_id in the config to later be able to call stop_broadcast()
        self.config[self.CONF_STREAM_SETTINGS][
            self.CONF_BROADCAST_ID
        ] = self.broadcast_id

        try:
            dirname = os.path.dirname(__file__)
            filename = os.path.join(dirname, self.config_file)
            with open(filename, "w") as f:
                json.dump(self.config, f, indent=4)
            self.logger.info(f"Broadcast ID stored in config file {filename}")
        except FileNotFoundError:
            self.logger.error(
                f"Could not save Broadcast ID. Config file {filename} not found."
            )

        self.logger.info(
            f"Binding stream ID `{self.config[self.CONF_STREAM_SETTINGS][self.CONF_STREAM_ID]}` Broadcast `{self.broadcast_id}`.."
        )
        if self._bind_broadcast_to_existing_stream() is None:
            self.logger.info("Binding stream to broadcast failed")
            return
        self.logger.info(
            "Stream was successfully bound to Broadcast - Broadcast should start as soon as you start streaming to the streaming-key provides by `create_stream`"
        )

        self.logger.info("Updating metadata..")
        if self.update_video_metadata() is None:
            self.logger.info("Updating metadata failed")
            return

        # check if broadcast status is `ready` and stream status is `active`
        # if it is we are already sending data to the stream and need to advance the stus manualy to start the broadcast
        # time.sleep(1)  # not sure if it is needed
        # if (
        #     self._check_broadcast_status() == "ready"
        #     and self._check_stream_health() == "active"
        # ):
        #     self._advance_broadcast()

    def stop_broadcast(self):
        # check if broadcast_id is set
        if self.config[self.CONF_STREAM_SETTINGS][self.CONF_BROADCAST_ID] is None:
            self.logger.error(
                "Failed to stop the Broadcast! It seems like no broadcast id is stored because no broadcast was started"
            )
            return None
        self._authenticate()
        try:
            request = self.youtube.liveBroadcasts().transition(
                part="status",
                broadcastStatus="complete",
                id=self.config[self.CONF_STREAM_SETTINGS][self.CONF_BROADCAST_ID],
            )
            response = request.execute()
            self.logger.info(
                f"Broadcast with  the id `{self.config[self.CONF_STREAM_SETTINGS][self.CONF_BROADCAST_ID]}` was stopped successfully"
            )
            return response
        except Exception as e:
            self.logger.error(f"Failed to stop broadcast: {str(e)}")
            return None

    def create_stream(self, name, stream_type, resolution, fps):
        self._authenticate()
        try:
            request = self.youtube.liveStreams().insert(
                part="snippet,cdn,contentDetails",
                body={
                    "snippet": {
                        "title": name,
                        "description": "Streaming with YouTube API",
                    },
                    "cdn": {
                        "ingestionType": stream_type,
                        "resolution": resolution,
                        "frameRate": f"{fps}fps",
                    },
                },
            )
            response = request.execute()
            self.logger.info(f"Stream created: {response}")
            return response
        except Exception as e:
            self.logger.error(f"Failed to create stream: {str(e)}")
            return None

    def _get_log_level(self):
        if self.config[self.CONF_LOGGER] == self.CONF_INFO:
            return logging.INFO
        elif self.config[self.CONF_LOGGER] == self.CONF_DEBUG:
            return logging.DEBUG
        return logging.INFO

    def _setup_logger(self):
        """Set up logging."""
        print(self._get_log_level())
        logging.basicConfig(
            level=self._get_log_level(),
            format="%(asctime)s - %(levelname)s - %(message)s",
        )
        dirname = os.path.dirname(__file__)
        filename = os.path.join(dirname, self.LOG_FILE)
        file_handler = logging.FileHandler(filename)
        file_handler.setLevel(self._get_log_level())
        file_handler.setFormatter(
            logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
        )
        logger = logging.getLogger()
        logger.addHandler(file_handler)
        return logger

    def _reload_log_level(self):
        """Dynamically update the log level."""
        self.logger.setLevel(self._get_log_level())
        for handler in self.logger.handlers:
            handler.setLevel(self._get_log_level())
        # print(f"Log level updated to {logging.getLevelName(self._get_log_level())}")

    def _load_config(self):
        """Load the configuration from a file."""
        try:
            dirname = os.path.dirname(__file__)
            filename = os.path.join(dirname, self.config_file)
            with open(filename, "r") as f:
                config = json.load(f)
            self.logger.info(f"Loaded configuration from {filename}")
            return config
        except FileNotFoundError:
            self.logger.error(f"Config file {filename} not found. - Exiting!!")
            exit(1)

    def _check_config(self) -> bool:
        if self.config[self.CONF_LOGGER] not in self.LOGGER_OPTIONS:
            self.logger.error(
                f"wrong log level set - options are: {self.LOGGER_OPTIONS}"
            )
            return False

        email_keys = [
            self.CONF_ENABLE_EMAIL,
            self.CONF_SMTP_SERVER,
            self.CONF_SMTP_PORT,
            self.CONF_SENDER_EMAIL,
            self.CONF_SENDER_PASSWORD,
            self.CONF_RECIPIENT_EMAIL,
            self.CONF_SUBJECT,
        ]
        if self.CONF_EMAIL not in self.config:
            self.logger.info("No email config is provided")
            self.config[self.CONF_EMAIL] = {self.CONF_ENABLE_EMAIL: False}
            self.config
        elif (
            self.CONF_ENABLE_EMAIL in self.config[self.CONF_EMAIL]
            and self.config[self.CONF_EMAIL][self.CONF_ENABLE_EMAIL]
        ):
            for key in email_keys:
                if key not in self.config[self.CONF_EMAIL]:
                    self.logger.error(
                        f"No `{key}` is provided under `{self.CONF_EMAIL}`"
                    )
                    return False

        if self.CONF_STREAM_SETTINGS not in self.config:
            self.logger.error("No `stream_settings` is provided")
            return False
        else:
            for key in self.DEFAULT_CONFIG[self.CONF_STREAM_SETTINGS]:
                if (
                    key == self.CONF_PRIVACY
                    and self.config[self.CONF_STREAM_SETTINGS][key]
                    not in self.CONF_PRIVACY_OPTIONS
                ):
                    self.logger.error(
                        f"Wrong {self.CONF_PRIVACY} option provided - valid options are: {self.CONF_PRIVACY_OPTIONS}"
                    )
                    return False
                if key not in self.config[self.CONF_STREAM_SETTINGS]:
                    # allow it to be None and check it in the functions chat requiere it
                    # if self.DEFAULT_CONFIG[self.CONF_STREAM_SETTINGS][key] is None:
                    #     self.logger.error(
                    #         f"`{key}` is requred under `{self.CONF_STREAM_SETTINGS}`"
                    #     )
                    #     return False
                    self.config[self.CONF_STREAM_SETTINGS][key] = self.DEFAULT_CONFIG[
                        self.CONF_STREAM_SETTINGS
                    ][key]

        if self.CONF_YOUTUBE_SETTINGS not in self.config:
            self.logger.error(f"No `{self.CONF_YOUTUBE_SETTINGS}` is provided")
            return False
        if self.CONF_CREDENTIALS_FILE not in self.config[self.CONF_YOUTUBE_SETTINGS]:
            self.logger.error(
                f"`{self.CONF_CREDENTIALS_FILE}` is requred under `{self.CONF_YOUTUBE_SETTINGS}`"
            )
            return False
        return True

    def _authenticate(self, create_new_token: bool = False):
        try:
            credentials = None
            dirname = os.path.dirname(__file__)
            token_filename = os.path.join(dirname, self.TOKEN_FILE)
            if os.path.exists(token_filename):  # do we have a token file allready?
                credentials = Credentials.from_authorized_user_file(
                    token_filename, self.SCOPES
                )

            if create_new_token:
                credentials = None  # will force to have to login again and create a new token file

            if (
                not credentials or not credentials.valid
            ):  # do we have no token or is the token no longer valid?
                if (
                    credentials and credentials.expired and credentials.refresh_token
                ):  # is the token expired?
                    try:
                        self.logger.debug("Token expired. Attempting to refresh...")
                        credentials.refresh(Request())
                        self.logger.debug("Token successfully refreshed.")
                    except Exception as e:
                        self.logger.error(f"Error renewing token: {e}")
                        raise e
                else:
                    try:
                        dirname = os.path.dirname(__file__)
                        filename = os.path.join(
                            dirname,
                            self.config[self.CONF_YOUTUBE_SETTINGS][
                                self.CONF_CREDENTIALS_FILE
                            ],
                        )
                        flow = InstalledAppFlow.from_client_secrets_file(
                            filename,
                            self.SCOPES,
                        )
                        credentials = flow.run_local_server(
                            port=0, access_type="offline", prompt="consent"
                        )
                    except Exception as e:
                        self.logger.error(
                            f"Failed to login, maybe the credentials file does not exist? `{filename}` Error was: {e}"
                        )
                        raise e
                try:
                    with open(token_filename, "w") as token:
                        token.write(credentials.to_json())
                        self.logger.info(
                            f"Token was succefully writen to token file `{token_filename}`"
                        )
                except Exception as e:
                    self.logger.error(
                        f"Failed to login, maybe the credentials file does not exist? `{token_filename}` Error was: {e}"
                    )
                    raise e

            self.youtube = build("youtube", "v3", credentials=credentials)
            return self.youtube
        except Exception as e:
            self.logger.error(f"Failed to authenticate: {str(e)}")
            return None

    def _create_live_broadcast(self):
        try:
            timezone = ZoneInfo("Europe/Berlin")
            current_time = datetime.datetime.now(timezone)
            current_date = datetime.datetime.now(timezone).strftime("%d.%m.%Y")
            if current_time.hour < 12:
                part = " (1)"
            else:
                part = " (2)"

            self.video_title = (
                self.config[self.CONF_STREAM_SETTINGS][self.CONF_TITLE]
                + " "
                + current_date
                + part
            )
            request = self.youtube.liveBroadcasts().insert(
                part="snippet,contentDetails,status",
                body={
                    "snippet": {
                        "title": self.video_title,
                        "description": self.config[self.CONF_STREAM_SETTINGS][
                            self.CONF_DESCRIPTION
                        ]
                        + "\n"
                        + current_date,
                        "scheduledStartTime": current_time.isoformat(),
                    },
                    "contentDetails": {
                        "monitorStream": {"enableMonitorStream": False},
                        "enableAutoStart": True,
                        "enableAutoStop": False,
                    },
                    "status": {
                        "privacyStatus": self.config[self.CONF_STREAM_SETTINGS][
                            self.CONF_PRIVACY
                        ],
                        "selfDeclaredMadeForKids": False,
                        "broadcastStatus": "upcoming",
                    },
                },
            )
            response = request.execute()
            self.logger.debug(response)
            if "id" not in response:
                self.logger.error("Broadcast creation failed: Response missing 'id'")
                return None
            self.broadcast_id = response["id"]
            return response
        except Exception as e:
            self.logger.error(f"Failed to create broadcast: {str(e)}")
            return None

    def _get_existing_stream(self, stream_id):
        request = self.youtube.liveStreams().list(
            part="snippet,cdn,contentDetails", id=stream_id
        )
        response = request.execute()
        self.logger.info("get_existing_setream response")
        self.logger.info(response)
        if response["items"]:
            return response["items"][0]
        else:
            self.logger.error(f"Stream with ID {stream_id} not found.")
            return None

    def _bind_broadcast_to_existing_stream(self):
        try:
            request = self.youtube.liveBroadcasts().bind(
                part="id,contentDetails",
                id=self.broadcast_id,
                streamId=self.config[self.CONF_STREAM_SETTINGS][self.CONF_STREAM_ID],
            )
            response = request.execute()
            self.logger.debug(response)

            if "id" in response and response["id"] == self.broadcast_id:
                self.logger.info(
                    f"Stream {self.config[self.CONF_STREAM_SETTINGS][self.CONF_STREAM_ID]} is correctly bound to Broadcast {self.broadcast_id}"
                )
            else:
                self.logger.error(
                    f"Failed to bind stream {self.config[self.CONF_STREAM_SETTINGS][self.CONF_STREAM_ID]} to Broadcast {self.broadcast_id}"
                )
            return response
        except Exception as e:
            self.logger.error(
                f"Error binding broadcast {self.broadcast_id} to stream {self.config[self.CONF_STREAM_SETTINGS][self.CONF_STREAM_ID]}: {e}"
            )

    def _check_stream_health(self):
        request = self.youtube.liveStreams().list(
            part="cdn, status",
            id=self.config[self.CONF_STREAM_SETTINGS][self.CONF_STREAM_ID],
        )
        response = request.execute()
        self.logger.debug(response)
        if response["items"]:
            cdn_settings = response["items"][0]["cdn"]
            health_status = response["items"][0]["status"].get(
                "streamStatus", "No Status"
            )
            self.logger.info(f"Stream Health: {health_status}")
            return health_status
        else:
            self.logger.error(
                f"Stream with ID {self.config[self.CONF_STREAM_SETTINGS][self.CONF_STREAM_ID]} not found."
            )
            return None

    def _check_broadcast_status(self):
        try:
            # Get the broadcast details to check if it's receiving a stream
            request = self.youtube.liveBroadcasts().list(
                part="status", id=self.broadcast_id
            )
            response = request.execute()

            # Check if the broadcast is ready and receiving a stream
            if response["items"]:
                status = response["items"][0]["status"]["lifeCycleStatus"]
                self.logger.info(f"Broadcast {self.broadcast_id} status: {status}")
                return status
            else:
                self.logger.error(f"Broadcast {self.broadcast_id} not found.")
                return None
        except HttpError as e:
            self.logger.error(f"Error checking broadcast status: {e}")
            return None

    def _advance_broadcast(self):
        try:
            # Transition the broadcast to "live"
            request = self.youtube.liveBroadcasts().transition(
                part="status", broadcastStatus="testing", id=self.broadcast_id
            )
            response = request.execute()
            self.logger.info(response)
            self.logger.info(f"Broadcast {self.broadcast_id} started manually.")
            return response
        except Exception as e:
            self.logger.error(f"Error starting broadcast {self.broadcast_id}: {e}")
            return None

    def update_video_metadata(self):
        try:
            snippet = {}
            snippet["title"] = self.video_title
            snippet["description"] = self.config[self.CONF_STREAM_SETTINGS][
                self.CONF_DESCRIPTION
            ]
            snippet["tags"] = self.config[self.CONF_STREAM_SETTINGS][self.CONF_TAGS]
            snippet["categoryId"] = self.config[self.CONF_STREAM_SETTINGS][
                self.CONF_CATEGORY
            ]

            request = self.youtube.videos().update(
                part="snippet",
                body={
                    "id": self.broadcast_id,
                    "snippet": snippet,
                },
            )
            response = request.execute()
            self.logger.debug(response)
            self.logger.info(
                f"Metadata succefully updated for Broadcast ID {self.broadcast_id}"
            )
            return response
        except Exception as e:
            self.logger.error(f"Failed to update Broadcast metadata: {str(e)}")
            return None


def main():
    import argparse

    parser = argparse.ArgumentParser(
        description="Manage YouTube streams and broadcasts."
    )
    parser.add_argument(
        "-config",
        type=str,
        default="config.json",
        help="Path to the configuration file (default: config.json)",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    # Sub-command: login
    start_broadcast_parser = subparsers.add_parser(
        "login",
        help="opens the browser to login an create a token.secret file after succefull login",
    )

    # Sub-command: create_stream
    create_stream_parser = subparsers.add_parser(
        "create_stream", help="Create a new stream"
    )
    create_stream_parser.add_argument("-name", required=True, help="Stream name")
    create_stream_parser.add_argument(
        "-streamType",
        required=True,
        choices=["rtmp", "hls"],
        help="Ingestion type for the stream",
    )
    create_stream_parser.add_argument(
        "-resolution",
        required=True,
        choices=["1440p", "1080p", "720p", "480p", "360p"],
        help="Stream resolution",
    )
    create_stream_parser.add_argument(
        "-fps", type=int, required=True, choices=[30, 60], help="Stream FPS"
    )

    # Sub-command: start_broadcast
    start_broadcast_parser = subparsers.add_parser(
        "start_broadcast",
        help="Creates a new broadcast, binds the set stream id, and starts it immediately",
    )

    # Sub-command: stop_broadcast
    stop_broadcast_parser = subparsers.add_parser(
        "stop_broadcast", help="Stops the currently running broadcast"
    )

    args = parser.parse_args()

    youtube = YouTubeStreamManager(args.config)
    if args.command == "login":
        youtube._authenticate(True)
    elif args.command == "create_stream":
        youtube.create_stream(args.name, args.streamType, args.resolution, args.fps)
    elif args.command == "start_broadcast":
        youtube.start_broadcast()
        # # while True:
        # youtube._check_broadcast_status()
        # youtube._check_stream_health()

        # time.sleep(5)
        # youtube._advance_broadcast()
    elif args.command == "stop_broadcast":
        youtube.stop_broadcast()


if __name__ == "__main__":
    main()
