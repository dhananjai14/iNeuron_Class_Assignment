from flask import Flask, request, jsonify
from DB_connection import sql
database = sql('localhost', 'root', '1212')

import pymongo
# Establishing the connectivity with the MangoDB atlas server
client = pymongo.MongoClient("mongodb+srv://dhan:1212@cluster0.n0zji.mongodb.net/?retryWrites=true&w=majority")
# Selecting the mongotest database
db_mongo = client['api']
coll = db_mongo['api']

import logging
logging.basicConfig(filename= 'assignment_20_08_22.log', level = logging.INFO,
                    format="%(levelname)s %(name)s %(asctime)s %(message)s ")

app = Flask(__name__)
@app.route('/sql/ques1' , methods = ['GET', 'POST'])
def ques():
    logging.info('\n\n')
    logging.info('New Log started for Ques 1')
    if (request.method == 'POST'):
        name = request.json['name']
        contact = request.json['contact']
        course = request.json['course']
        emailID = request.json['email']
        logging.info('User input name: {}, contact: {}, course: {}, emailID {}'.format(name, contact, course, emailID))
        try:
            query1 = '''INSERT INTO assignment1.api VALUE ('{}', '{}','{}', '{}' ) '''.format(name, contact, course, emailID)
            logging.info("{} generated".format(query1))
            database.query_execute(query1)
            logging.info("query1 executed")
            return jsonify(('Query successfully executed'))
        except Exception as e:
            logging.exception(e)
            return jsonify((str(e)))


@app.route('/sql/ques2' , methods=['GET' , 'POST'])
def ques2():
    logging.info('\n\n')
    logging.info('New Log started for Ques 2')
    if (request.method == 'POST'):
        try:
            name = request.json['name']
            phone = request.json['phone']
            course = request.json['course']
            mail = request.json['mail']
            logging.info('User input name: {}, contact: {}, course: {}, emailID {}'.format(name, phone, course, mail))
            query1 = """UPDATE assignment1.api 
                        SET Name = '{}' , course = '{}' , emailID = '{}' 
                        WHERE phone = {} """.format(name, course, mail, phone)
            logging.info('Query generated: {}'.format(query1))
            database.query_execute(query1)
            logging.info('Query Executed Successfully')
            return jsonify('data updated')
        except Exception as e:
            logging.exception(e)
            return jsonify((str(e)))

@app.route('/sql/ques3' , methods = ['GET' , 'POST'])
def ques3():
    logging.info('\n\n')
    logging.info('New Log started for Ques 3')
    if (request.method == 'POST'):
        try:
            phone = request.json['phone']
            logging.info('User input  contact: {}'.format(phone))
            query1 = """DELETE FROM assignment1.api WHERE phone = {}""".format(phone)
            logging.info('Query generated: {}'.format(query1))
            database.query_execute(query1)
            logging.info('Query executed')
            return jsonify(('Data deleted'))
        except Exception as e:
            logging.exception(e)
            return jsonify((str(e)))

@app.route('/sql/ques4' , methods = ['GET' , 'POST'])
def ques4():
    logging.info('\n\n')
    logging.info('New Log started for Ques 4')
    if (request.method == 'POST'):
        try:
            phone = request.json['phone']
            logging.info('User input contact: {}'.format( phone))
            query1 = '''SELECT * FROM assignment1.api WHERE phone = {}'''.format(phone)
            logging.info('Query generated: {}'.format(query1))
            database.query_execute(query1)
            logging.info('Query executed')
            return jsonify(('Data extracted'))
        except Exception as e:
            logging.exception(e)
            return jsonify((str(e)))

@app.route('/mongo/ques5' , methods=['GET' , 'POST'])
def ques5():
    logging.info('\n\n')
    logging.info('New Log started for Ques 5')
    if (request.method == 'POST'):
        try:
            query = {
                'Name': request.json['name'],
                'Phone_no': request.json['contact'],
                'Course': request.json['course'],
                'EmailID': request.json['email']
            }
            logging.info('Query generated: {}'.format(query))
            coll.insert_one(query)
            logging.info('Query executed')
            return jsonify('Query successfully executed')
        except Exception as e:
            logging.exception(e)
            return jsonify((str(e)))

@app.route('/mongo/ques6', methods = ['GET', 'POST'])
def ques6():
    logging.info('\n\n')
    logging.info('New Log started for Ques 6')
    if (request.method == 'POST'):
        try:
            query = {
                'Phone_no': request.json['Phone_no']}
            newval = {"$set":{'Name': request.json['name'],
                 'Course': request.json['course'],
                 'EmailID': request.json['email']}}
            logging.info('Query generated: {}'.format(query))
            coll.update_one(query, newval)
            logging.info('Query executed')
            return jsonify('Query updated successfully')
        except Exception as e:
            logging.exception(e)
            return jsonify((str(e)))

@app.route('/mongo/ques7', methods = ['GET', 'POST'])
def ques7():
    logging.info('\n\n')
    logging.info('New Log started for Ques 7')
    if (request.method == 'POST'):
        try:
            query = {
                'Phone_no': request.json['phone']
            }
            logging.info('Query generated: {}'.format(query))
            coll.delete_one(query)
            logging.info('Query executed')
            return jsonify('Data deleted')
        except Exception as e:
            logging.exception(e)
            return jsonify((str(e)))

@app.route('/mongo/ques8', methods = ['GET', 'POST'])
def ques8():
    logging.info('\n\n')
    logging.info('New Log started for Ques 8')
    if (request.method == 'POST'):
        try:
            query = {
                'Phone_no': request.json['phone']
            }
            logging.info('Query generated: {}'.format(query))
            coll.find({}, query)
            logging.info('Query executed')
            return jsonify('Data has extracted')
        except Exception as e:
            logging.exception(e)
            return jsonify((str(e)))

if __name__ == '__main__':
    app.run(debug=True)


