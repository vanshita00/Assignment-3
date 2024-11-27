# Write a program to read the json data from that dataset and insert into these table data_date should be the current date always when the programe is running,

# schedule_revision_details:
# source name = 'WR'
# data_date = 'CURRENT_DATE'
# revision_no = (If record for same source name and same date already exist then increase revision no by 1 else 0)

import json
import mysql.connector
from datetime import datetime

def manage_schedule_data(json_file_path,source_name):

    with open(json_file_path,'r')as file:
        data=json.load(file)

    database=mysql.connector.connect(
        host="localhost",
        user="root",
        password="vanshita1234@",
        database="schedule"
    )
    db=database.cursor()
    db.execute('''CREATE TABLE IF NOT EXISTS schedule_revision_details (
        id INT AUTO_INCREMENT PRIMARY KEY,
        source_name VARCHAR(255),
        data_date DATE,
        revision_no INT)''')

    current_date = datetime.now().date()
    for record in data:
        db.execute('''SELECT MAX(revision_no)FROM schedule_revision_details WHERE source_name=%s AND data_date=%s''',(source_name, current_date))
        result=db.fetchall()
        revision_no=(result[0][0]+1)if result and result[0][0]is not None else 1

        db.execute('''INSERT INTO schedule_revision_details(source_name, data_date, revision_no)VALUES(%s, %s, %s)''',(source_name, current_date, revision_no))
result=manage_schedule_data("data_set_python_training.json","WR")

