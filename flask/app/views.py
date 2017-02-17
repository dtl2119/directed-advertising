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
#    user = { 'nickname': 'Miguel' } # fake user
#    user = None
#    mylist= [1,2,3,4]
    return render_template("index.html")




#@app.route('/table')
#def table():
#    return render_template("table.html")
#
#@app.route("/table", methods=['POST'])
#def table_post():
#    stmt = "select * from usersearches limit 20"
#    #response = session.execute(stmt, parameters=[userid, categoryid])
#    response = session.execute(stmt)
#    response_list = []
#    for r in response:
#        response_list.append(r)
#        jsonresponse = [{"userid": x.userid, "user": x.user, "categoryid": x.categoryid, "time": x.time, "productid": x.productid} for x in response_list]
#    print jsonresponse
#    #return jsonify(searches=jsonresponse)
#    #return jsonify("tableop.html", output=jsonresponse)
#    return jsonify("tableop.html")

#@app.route("/table")
@app.route("/searchestable")
def table_post():
    stmt = "select * from usersearches limit 20"
    #response = session.execute(stmt, parameters=[userid, categoryid])
    response = session.execute(stmt)
    response_list = []
    for r in response:
        response_list.append(r)
        jsonresponse = [{"userid": x.userid, "user": x.user, "categoryid": x.categoryid, "time": x.time, "productid": x.productid} for x in response_list]
    #print jsonresponse
    return render_template("table.html", output=jsonresponse)





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
    stmt = "SELECT * FROM usersearches WHERE id=%s and date=%s"
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


#@app.route('/')
#@app.route('/index')
@app.route('/querysearches')
def search():
    return render_template("searches.html")

@app.route("/querysearches", methods=['POST'])
def searches_post():
    form_userid = int(request.form["userid"])
    form_categoryid = int(request.form["categoryid"])

    stmt = "SELECT * FROM usersearches WHERE userid = %s and categoryid = %s"
    parameters = (form_userid, form_categoryid)
    response = session.execute(stmt, parameters)
    response_list = []
    for val in response:
        response_list.append(val)
        jsonresponse = [{"userid": x.userid, "user": x.user, "categoryid": x.categoryid, "productid": x.productid} for x in response_list]
    print jsonresponse
    return render_template("searchesop.html", output=jsonresponse)
