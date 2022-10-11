import os

from flask import Flask
from main import internal_check_migs

app = Flask(__name__)


@app.route("/")
def check_mig():
    project = os.environ.get("PROJECT", "-")
    region = os.environ.get("REGION", "-")

    enriched_instance_group_managers2 = {'instanceGroupManagers': {}, 'result': 'ok'}
    internal_check_migs(project, region ,enriched_instance_group_managers2)

    return "Hello %s %s !\n\n"%(project,region)


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
