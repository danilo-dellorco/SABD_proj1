from http.server import BaseHTTPRequestHandler, HTTPServer
import subprocess
import urllib.request
import requests

class handler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header('Content-type','text/html')
        self.end_headers()

        message = "Hello, World! Here is a GET response"
        self.wfile.write(bytes(message, "utf8"))

    def do_POST(self):
        resp = requests.post("http://localhost:8090/nifi-api/flowfile-queues/fbead702-0180-1000-8839-894da921bdb4/listing-requests").json()
        print(resp)
        flow_id = resp.get("listingRequest").get("id")
        print(flow_id)
        filename = requests.get("http://localhost:8090/nifi-api/flowfile-queues/fbead702-0180-1000-8839-894da921bdb4/listing-requests/" + flow_id).json().get("listingRequest").get("flowFileSummaries")
        print(filename)
        porcodio = filename[0].get("filename")
        if (porcodio == "yellow.parquet"):
            subprocess.call(['sh', './stop-nifi-flow.sh'])
            # cambiare filename attribute per il flusso nuovo verde
        
        subprocess.call(['sh', './stop-nifi-flow.sh'])


        
    

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