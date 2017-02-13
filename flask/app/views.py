# jsonify creates a json representation of the response
from flask import jsonify
from flask import render_template, request

from app import app

# importing Cassandra modules from the driver we just installed
from cassandra.cluster import Cluster

# Setting up connections to cassandra

#cluster = Cluster(['<seed-node-public-dns>'])
cluster = Cluster(['34.199.42.65'])

# Change the bolded text to the keyspace which has the table you want to query. Same as above for < or > and quotations. Also delete this comment line
session = cluster.connect('advertise')



@app.route('/')
@app.route('/index')
def index():
    user = { 'nickname': 'Miguel' } # fake user
    user = None
    mylist= [1,2,3,4]
    return render_template("index.html", title = 'Home', user = user, mylist = mylist)

#@app.route('/api/<userid>/<categoryid>')
#def get_searches(userid, categoryid):
#    stmt = "SELECT * FROM usersearches WHERE userid=%s and categoryid=%s"
#    response = session.execute(stmt, parameters=[userid, categoryid])
#    response_list = []
#    for r in response:
#        response_list.append(r)
#    #jsonresponse = [{"first name": x.fname, "last name": x.lname, "id": x.id, "message": x.message, "time": x.time} for x in response_list]
#    #jsonresponse = [{"searches": ", ".join(list(x.searches))} for x in response_list]
#    jsonresponse = [{"searches": list(x.searches)}  for x in response_list]
#    #jsonresponse = [{"userid": x.userid, "categoryid": x.categoryid, "searches": ", ".join(list(x.searches))} for x in response_list]
#    #return jsonify(searches=jsonresponse)
#    return jsonify(jsonresponse)


@app.route('/api/<email>/<date>')
def get_email(email, date):
       stmt = "SELECT * FROM email WHERE id=%s and date=%s"
       response = session.execute(stmt, parameters=[email, date])
       response_list = []
       for val in response:
            response_list.append(val)
       jsonresponse = [{"first name": x.fname, "last name": x.lname, "id": x.id, "message": x.message, "time": x.time} for x in response_list]
       return jsonify(emails=jsonresponse)



# Template just for the twitter bootstrap simple server
#@app.route('/email')
#def email():
#     return render_template("base.html")



@app.route('/email')
def email():
 return render_template("email.html")

@app.route("/email", methods=['POST'])
def email_post():
 emailid = request.form["emailid"]
 date = request.form["date"]

 #email entered is in emailid and date selected in dropdown is in date variable respectively

 stmt = "SELECT * FROM email WHERE id=%s and date=%s"
 response = session.execute(stmt, parameters=[emailid, date])
 response_list = []
 for val in response:
    response_list.append(val)
 jsonresponse = [{"fname": x.fname, "lname": x.lname, "id": x.id, "message": x.message, "time": x.time} for x in response_list]
 return render_template("emailop.html", output=jsonresponse)
