from http.server import BaseHTTPRequestHandler, HTTPServer
import subprocess
import urllib.request

class handler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header('Content-type','text/html')
        self.end_headers()

        message = "Hello, World! Here is a GET response"
        self.wfile.write(bytes(message, "utf8"))

    def do_POST(self):
        subprocess.call(['sh', './stop-nifi-flow.sh'])
    

with HTTPServer(('172.17.0.1', 5555), handler) as server:
    #subprocess.call(['sh', './start-hdfs.sh'])

    # start nifi flow when the web UI is up and running
    message = 0
    while (message != 200):
        message = urllib.request.urlopen("http://localhost:8090/nifi").getcode()
        print(message)
    subprocess.call(['sh', './start-nifi-flow.sh'])
    
    server.serve_forever()