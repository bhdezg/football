import mysql.connector

cnx = mysql.connector.connect(
    host="HOST",
    user="USER",
    password="PASSWORD",
    database="DB"
)

cursor = cnx.cursor()
query = "DROP TABLE goals_team"
cursor.execute(query)
cnx.commit()
cursor.close()

cursor = cnx.cursor()
cursor.callproc("CreateGoalsTeamTable")

for result in cursor.stored_results():
    rows = result.fetchall()
    for row in rows:
        print(row)

cnx.commit()
cursor.close()
