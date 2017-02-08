# jsonify creates a json representation of the response
from flask import render_template
from flask import jsonify

from app import app

# importing Cassandra modules from the driver we just installed
from cassandra.cluster import Cluster

# Setting up connections to cassandra
# Public DNS of seed node (master 1)
cluster = Cluster(['ec2-34-199-42-65.compute-1.amazonaws.com'])

# Change the bolded text to the keyspace which has the table you want to query. Same as above for < or > and quotations. Also delete this comment line
session = cluster.connect('advertise')

@app.route('/')
@app.route('/index')
def index():
  #return "Hello, World!"
    user = { 'nickname': 'Miguel' } # fake user
    return render_template("index.html", title = 'Home', user = user)


@app.route('/api/<email>/<date>')
def get_email(email, date):
       stmt = "SELECT * FROM usersearches"
       response = session.execute(stmt, parameters=[userid, categoryid])
       response_list = []
       for val in response:
            response_list.append(val)
       jsonresponse = [{"first name": x.fname, "last name": x.lname, "id": x.id, "message": x.message, "time": x.time} for x in response_list]
       return jsonify(emails=jsonresponse)




## Hello World tutorial
#from app import app
#
##@app.route('/directed-advertising')
#@app.route('/')
#@app.route('/index')
#def index():
#    return "Hello, World!"
