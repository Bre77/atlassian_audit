import os
import sys
import json
import time
from datetime import datetime, timedelta

# Prepend the bundled lib dir BEFORE importing third-party packages so the
# add-on uses its own requests/certifi/urllib3 instead of the Splunk platform's.
# (Splunk 10 ships OpenSSL 3.0 + Python 3.9; relying on platform libs breaks TLS.)
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "lib"))

import requests
import certifi
from splunk_input_runtime.modularinput import Argument, Event, EventWriter, Scheme, Script


class Input(Script):
    MASK = "<encrypted>"
    APP = "atlassian_audit"

    def get_scheme(self):
        scheme = Scheme("Atlassian Audit")
        scheme.description = "Pull audit events from Atlassian Cloud"
        scheme.use_external_validation = False
        scheme.streaming_mode_xml = True
        scheme.use_single_instance = False

        scheme.add_argument(Argument(
            name="org",
            title="Organisation ID",
            data_type=Argument.data_type_string,
            required_on_create=True,
            required_on_edit=False,
        ))
        scheme.add_argument(Argument(
            name="key",
            title="API Key",
            data_type=Argument.data_type_string,
            required_on_create=True,
            required_on_edit=False,
        ))
        scheme.add_argument(Argument(
            name="history",
            title="Days of historical data",
            data_type=Argument.data_type_number,
            required_on_create=False,
            required_on_edit=False,
        ))
        return scheme

    def stream_events(self, inputs, ew):
        # Get Variables
        input_name, input_items = inputs.inputs.popitem()
        kind, name = input_name.split("://")
        url = f"https://api.atlassian.com/admin/v1/orgs/{input_items['org']}/events"
        source = f"api.atlassian.com/admin/v1/orgs/{input_items['org']}/events"
        checkpointfile = os.path.join(
            self._input_definition.metadata["checkpoint_dir"], name
        )

        # Password Encryption
        secrets = self.context.credentials.protect_input_fields(
            kind=kind,
            stanza=name,
            values=input_items,
            fields=("key",),
            placeholder=self.MASK,
        )
        input_items["key"] = secrets["key"]

        # Checkpoint
        first = "0"
        try:
            with open(checkpointfile, "r") as f:
                last = json.load(f)
        except Exception as e:
            ew.log(EventWriter.INFO, f"Checkpoint not found: {e}")
            days = timedelta(days=float(input_items.get("history",7)))
            last = datetime.strftime(datetime.now() - days,"%Y-%m-%dT%H:%M:%S.%fZ")
        ew.log(EventWriter.INFO, f"Will grab events since {last}")

        count = 0

        # Get Data
        with requests.Session() as session:
            session.verify = certifi.where()
            session.headers.update({'Accept': 'application/json', 'Authorization': "Bearer "+input_items["key"]})
            while url:
                with session.get(url) as r:
                    if not r.ok:
                        if r.status_code == 429:
                            wait = int(r.headers.get('X-Retry-After',30))
                            ew.log(EventWriter.INFO, f"Rate limited, waiting {wait} seconds")
                            time.sleep(wait)
                            continue
                        else:
                            ew.log(EventWriter.ERROR, f"Failed to get data from Atlassian: {r.text}")
                            return
                    
                    resp = r.json()
                    url = resp['links'].get('next')

                    for event in resp['data']:
                        datestr = event['attributes']['time']
                        if datestr > last:
                            count += 1
                            ew.write_event(
                                Event(
                                    time=datetime.strptime(datestr,"%Y-%m-%dT%H:%M:%S.%fZ").timestamp(),
                                    source=source,
                                    data=json.dumps(event, separators=(",", ":")),
                                )
                            )
                            if datestr > first:
                                ew.log(EventWriter.DEBUG, f"Starting with {datestr}")
                                first = datestr
                                with open(checkpointfile, "w") as f:
                                    json.dump(first, f)
                        else:
                            ew.log(EventWriter.DEBUG, f"Stopping before {datestr}")
                            url = False
                            break
        
        ew.log(EventWriter.INFO, f"Atlassian Audit collected {count} events between {first} and {last}.")
            

if __name__ == "__main__":
    exitcode = Input().run(sys.argv)
    sys.exit(exitcode)