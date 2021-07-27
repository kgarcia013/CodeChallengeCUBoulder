import yaml
import pandas as pd
import sqlite3



def InsertNewStudentRecord(newstudents, con):
    """
    :param newstudents:
    :param con:
    :return:
    """

    def GetMaxStudent(con):
        """
        :param con:
        :return max_student:
        """
        SQL = "select max(id) from student"

        cursor = con.cursor()
        cursor.execute(SQL)

        max_student = cursor.fetchall()
        max_student = max_student[0][0]

        return max_student

    def GetMaxStudentMajor(con):
        """
        :param con:
        :return max_student_major:
        """
        SQL = "select max(id) from student_major"

        cursor = con.cursor()
        cursor.execute(SQL)

        max_student_major = cursor.fetchall()
        max_student_major = max_student_major[0][0]

        return max_student_major

    for key, value in newstudents.items():

        student_id = GetMaxStudent(con) + 1
        first_name = value['FirstName']
        last_name = value['LastName']
        dob = value['DOB']

        cursor = con.cursor()

        insert_student_sql = "INSERT INTO student (id, first_name, last_name, dob) values ({0}, '{1}', '{2}', '{3}' )".format(student_id, first_name, last_name, dob)
        cursor.execute(insert_student_sql)
        print(insert_student_sql)

        con.commit()

def main():
    con = sqlite3.connect("student_major.db")

    """
    Insert New Student Records from Yaml File Provided in newstudent.yaml
    """
    # newstudents = yaml.load(open('.\\newstudent.yaml', 'r'))
    # InsertNewStudentRecord(newstudents, con)




if __name__ == "__main__":
    main()