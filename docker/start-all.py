from http.server import BaseHTTPRequestHandler, HTTPServer
import subprocess
import urllib.request
import requests
import os

class handler(BaseHTTPRequestHandler):
    count = 0;

    def do_PUT(self):
        # first flow for yellow-taxi dataset
        if (self.count == 0):
            subprocess.call(['sh', './stop-nifi-flow.sh'])
            resp = requests.put("http://localhost:8090/nifi-api/55931828-e88b-3257-8990-11b8066f15d9", data = {'filename' : 'green-taxi.parquet'})
            print(resp.content)
            self.count += 1
    
        # second flow for green-taxi dataset, then restore attribute filename for new executions
        print("so arrivato quaaaaaaaaaaaaaaaaaaaaaaaaa")
        subprocess.call(['sh', './stop-nifi-flow.sh'])
        resp = requests.put("http://localhost:8090/nifi-api/55931828-e88b-3257-8990-11b8066f15d9", data = {'filename' : 'yellow-taxi.parquet'})
        os.exit()


        
    

with HTTPServer(('172.17.0.1', 5555), handler) as server:
    subprocess.call(['sh', './start-hdfs.sh'])

    # start nifi flow when the web UI is up and running
    message = 0
    while (message != 200):
        try:
            message = urllib.request.urlopen("http://localhost:8090/nifi").getcode()
            print(message)
        except:
            print("Nifi not running...")
        
    subprocess.call(['sh', './start-nifi-flow.sh'])
    server.serve_forever()