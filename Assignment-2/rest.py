from flask import Flask
from flask_restful import Api, Resource, reqparse
from flask_cors import CORS
import psycopg2

app = Flask(__name__)
api = Api(app)
CORS(app)

class Users(Resource):
    def get(self):
        #####################################################################################3
        #### Important -- This is the how the connection must be done for autograder to work
        ### But on your local machine, you may need to remove "host=..." line if this doesn't work
        #####################################################################################3
        #conn = psycopg2.connect("host=127.0.0.1 dbname=socialnetwork user=vagrant password=vagrant")
        conn = psycopg2.connect("dbname=socialnetwork host=127.0.0.1 user=vagrant password=vagrant")
        cur = conn.cursor()

        cur.execute("select * from users;")
        d = {"tuples": []}
        for c in cur.fetchall():
            t_to_d = {"userid": c[0], "name": c[1], "birthdate": str(c[2]), "joined": str(c[3])}
            d["tuples"].append(t_to_d)
        return d,200

class User(Resource):
    # Return all the info about a specific user, including its friends as an array
    # FORMAT: {"userid": "user0", "name": "...", "birthdate": "...", "joined": "...", "friends": ["friendname1", "friendname2", ...]}
    def get(self, userid):
        # Add your code to construct "ret" using the format shown below
        # Friend names must be sorted in alphabetically increasing order
        # Birthdate should be of the format: "2007-02-04" (this is what Python str() will give you)
        conn = psycopg2.connect("dbname=socialnetwork host=127.0.0.1 user=vagrant password=vagrant")
        cur = conn.cursor()

        cur.execute(f"select * from users where userid = '{userid}';")
        user_data = cur.fetchone()
        cur.execute(f"select name from friends, users where friends.userid1 = '{userid}' and friends.userid2 = users.userid order by name;")
        user_friends = list(sum(cur.fetchall(),()))
        print(user_friends)
        ret = {"userid": user_data[0], "name": user_data[1], "birthdate": str(user_data[2]), "joined": str(user_data[3]), "friends": user_friends}
        return ret, 200

    # Add a new user into the database, using the information that's part of the POST request
    # We have provided the code to parse the POST payload
    # If the "userid" is already present in the database, a FAILURE message should be returned
    def post(self, userid):
        conn = psycopg2.connect("dbname=socialnetwork host=127.0.0.1 user=vagrant password=vagrant")
        cur = conn.cursor()
        
        parser = reqparse.RequestParser()
        parser.add_argument("name")
        parser.add_argument("birthdate")
        parser.add_argument("joined")
        args = parser.parse_args()
        print(args)

        # Add your code to check if the userid is already present in the database
        cur.execute(f"select * from users where userid = '{userid}';")
        userid_already_present = (len(cur.fetchall()) > 0)

        if userid_already_present:
            return "FAILURE -- Userid must be unique", 201
        else:
            # Add your code to insert the new tuple into the database
            cur.execute(f"insert into users values ('{userid}','{args['name']}','{args['birthdate']}','{args['joined']}');")
            conn.commit()
            return "SUCCESS", 201

    # Delete the user with the specific user id from the database
    def delete(self, userid):
        conn = psycopg2.connect("dbname=socialnetwork host=127.0.0.1 user=vagrant password=vagrant")
        cur = conn.cursor()

        # Add your code to check if the userid is present in the database
        cur.execute(f"select * from users where userid='{userid}';")
        userid_present = (len(cur.fetchall()) > 0)

        if userid_present:
            # Add your code to delete the user from all of the tables, including
            # friends, users, follows, status, members, likes, etc.
            cur.execute(f"delete from friends where userid1='{userid}' or userid2='{userid}';")
            conn.commit()
            cur.execute(f"delete from follows where userid1='{userid}' or userid2='{userid}';")
            conn.commit()
            cur.execute(f"delete from members where userid='{userid}';") 
            conn.commit()
            cur.execute(f"delete from likes where userid='{userid}';")
            conn.commit()
            # delete all likes from people who liked statuses posted by this person
            cur.execute(f"select statusid from status where userid='{userid}'")
            for status in cur.fetchall():
                cur.execute(f"delete from likes where statusid='{status[0]}'")
            cur.execute(f"delete from status where userid='{userid}';")
            conn.commit()
            cur.execute(f"delete from users where userid='{userid}';")
            conn.commit()
            return "SUCCESS", 201
        else:
            return "FAILURE -- Unknown Userid", 404
      
api.add_resource(Users, "/users/")
api.add_resource(User, "/user/<string:userid>")

app.run(debug=True, host="0.0.0.0", port=5000)
