import logging
from typing import Any
import renderapi
import requests

class Connecter:
    """Talks with the render-ws rest api

    host: url of server
    owner: name of project owner
    project: project name
    client_scripts: location of render java client scripts
    auth: http basic auth credentials, tuple of (username, password)
    """

    def __init__(self, host, owner, project, auth=None):
        session = requests.Session()
        session.auth = auth
        self.render = dict(
            host=host, owner=owner, project=project, session=session
        )
        self.host = host
        self.owner = owner
        self.project = project

        logging.info(
            f"connecting to {self.host} for {self.owner} "
            f"in {self.project}"
            )
        
    def get_render_info(self):
        return self.render

