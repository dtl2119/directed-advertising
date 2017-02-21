
from flask import jsonify
from flask import render_template, request
from app import app
from cassandra.cluster import Cluster

# Setting up connections to cassandra

cluster = Cluster(['34.199.42.65'])

session = cluster.connect('advertise')



@app.route('/')
@app.route('/index')
def index():
    return render_template("index.html")

#@app.route("/table")
@app.route("/searchestable")
def table_post():
    stmt = "select * from usersearches limit 50"
    #response = session.execute(stmt, parameters=[userid, categoryid])
    response = session.execute(stmt)
    response_list = []
    for r in response:
        response_list.append(r)
        jsonresponse = [{"userid": x.userid, "user": x.user, "categoryid": x.categoryid, "time": x.time, "productid": x.productid} for x in response_list]
    #print jsonresponse
    return render_template("table.html", output=jsonresponse)


@app.route('/api/<email>/<date>')
def get_email(email, date):
    stmt = "SELECT * FROM usersearches WHERE id=%s and date=%s"
    response = session.execute(stmt, parameters=[email, date])
    response_list = []
    for val in response:
        response_list.append(val)
    jsonresponse = [{"first name": x.fname, "last name": x.lname, "id": x.id, "message": x.message, "time": x.time} for x in response_list]
    return jsonify(emails=jsonresponse)



@app.route('/querysearches')
def search():
    return render_template("searches.html")

@app.route("/querysearches", methods=['POST'])
def searches_post():
    form_userid = int(request.form["userid"])
    form_categoryid = int(request.form["categoryid"])

    stmt = "SELECT * FROM usersearches WHERE userid = %s and categoryid = %s LIMIT 3;"
    parameters = (form_userid, form_categoryid)
    response = session.execute(stmt, parameters)
    response_list = []
    for val in response:
        response_list.append(val)
        jsonresponse = [{"userid": x.userid, "user": x.user, "categoryid": x.categoryid, "time":x.time, "productid": x.productid} for x in response_list]
    print jsonresponse
    return render_template("searchesop.html", output=jsonresponse)


@app.route('/queryuser')
def usersearch():
    return render_template("user.html")

@app.route("/queryuser", methods=['POST'])
def user_post():
    form_userid = int(request.form["userid"])

    stmt = "SELECT * FROM searches WHERE userid = %s LIMIT 4;" % form_userid
    response = session.execute(stmt)
    response_list = []
    for val in response:
        response_list.append(val)
        jsonresponse = [{"userid": x.userid, "user": x.user, "categoryid": x.categoryid, "time":x.time, "productid": x.productid} for x in response_list]
    print jsonresponse
    return render_template("userop.html", output=jsonresponse)



@app.route('/queryproduct')
def productsearch():
    return render_template("product.html")

@app.route("/queryproduct", methods=['POST'])
def product_post():
    form_categoryid = int(request.form["categoryid"])

    stmt = "SELECT * FROM productcount WHERE categoryid = %s LIMIT 4;" % form_categoryid
    response = session.execute(stmt)
    response_list = []
    for val in response:
        response_list.append(val)
        jsonresponse = [{"categoryid": x.categoryid, "count":x.count, "productid": x.productid} for x in response_list]
    print jsonresponse
    return render_template("productop.html", output=jsonresponse)
