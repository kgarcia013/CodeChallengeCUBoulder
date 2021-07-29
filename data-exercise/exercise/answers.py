import yaml
import pandas as pd
import sqlite3
import csv
import copy


def GetMaxStudent(con):
    """
    Gets the max student id in the student table and returns it

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
    Gets the max student id in the student_major table and returns it

    :param con:
    :return max_student_major:

    """
    SQL = "select max(id) from student_major"

    cursor = con.cursor()
    cursor.execute(SQL)

    max_student_major = cursor.fetchall()
    max_student_major = max_student_major[0][0]

    return max_student_major


def InsertNewStudentRecord(newstudents, max_student,con):
    """
    :param newstudents:
    :param con:
    :return:
    """

    for key, value in newstudents.items():

        student_id = max_student + 1
        first_name = value['FirstName']
        last_name = value['LastName']
        dob = value['DOB']

        cursor = con.cursor()

        insert_student_sql = "INSERT INTO student (id, first_name, last_name, dob) values ({0}, '{1}', '{2}', '{3}' )".format(student_id, first_name, last_name, dob)
        cursor.execute(insert_student_sql)

        max_student = max_student + 1

        con.commit()


def CreateMajorsDict(con):
    """
    Creates a Dictionary of Majors using pandas.
    The Sqlite Query is stored in a pandas dataframe and then converted to a dictionary
    :param con:
    :return department_dict:
    """
    SQL = """select id, major_name from major"""

    majors_df = pd.read_sql_query(SQL, con)
    majors_df = majors_df.set_index('id')
    majors_dict = majors_df.to_dict()

    return majors_dict


def CreateDepartmentDict(con):
    """
    Creates a Dictionary of Departments using pandas.
    The Sqlite Query is stored in a pandas dataframe and then converted to a dictionary
    :param con:
    :return department_dict:

    """

    SQL = """select id, department_name from department"""

    department_df = pd.read_sql_query(SQL, con)
    department_df = department_df.set_index('id')
    department_dict = department_df.to_dict()

    return department_dict

def QuestionOne(con):
    """
    This is the solution to question one.

    It grabs the query from sqlite, stores the output into a pandas DataFrame and stores it in a csv

    question_one_output.csv

    :param con:
    :return:
    """

    SQL ="""select first_name, last_name, m.major_name from student s
    join student_major sm on s.id = sm.student_id
    join major m on m.id = sm.major_id
    join department d on d.id = m.department_id
    where department_name in ('Engineering', 'Language Arts')
    order by last_name"""

    question_one_df = pd.read_sql_query(SQL, con)
    question_one_df.to_csv('question_one_output.csv', index=False)


def QuestionTwo(con, majors_dict,  department_dict):

    """

    :param con:
    :param majors_dict:
    :param department_dict:
    :return:

    """

    def StudentsPerMajor(con):
        """
        Queries SQLite to Grab the number of students per major. The output is  then stored in a pandas dataframe.

        In order to get the majors that didn't have any students, I compared the the dataframe to the majors_dict and stored the remaining majors into the dataframe.

        Dataframe is then outputted into a csv file.

        :param con:
        :return:

        """
        SQL = """select m.id, m.major_name, count(*) as 'students_per_major' from student s
        left join student_major sm on s.id = sm.student_id
        left join major m on m.id = sm.major_id
        group by  m.id"""

        question_two_df = pd.read_sql_query(SQL, con)
        #Turns None Values in the id column to 0
        question_two_df['id'] = question_two_df['id'].fillna(0)
        #Stores values in id to be integers (previously stored as floats)
        question_two_df['id'] = question_two_df['id'].astype(int)
        #Replaces any remaining values with 'Students with No Major'
        question_two_df = question_two_df.replace([None], 'Students with No Major')

        # Loops through majors dictionary to determine which majors are not present in the database.
        for key, value in majors_dict['major_name'].items():
            major_id = int(key)
            major_name = value

            result = question_two_df.isin([major_name]).any().any()

            if str(result) == 'False':
                #Adds new a row into the dataframe to show which majors have zero students
                temp_df = {'id': major_id, 'major_name': major_name, 'students_per_major': 0}
                question_two_df = question_two_df.append(temp_df, ignore_index=True)

        #Stores dataframe output into a csv
        question_two_df.to_csv('question_two_output_student_per_major.csv', index=False)

    def StudentsPerDepartment(con):
        """
        Queries SQLite to Grab the number of students per Department. The output is  then stored in a pandas dataframe.

        In order to get the Departments that didn't have any students, I compared the the dataframe to the department_dict and stored the remaining majors into the dataframe.

        Dataframe is then outputted into a csv file.

        :param con:
        :return:
        """

        SQL = """select d.id, d.department_name, count(*) as 'students_per_department' from student s
        left join student_major sm on s.id = sm.student_id
        left join major m on m.id = sm.major_id
        left join department d on d.id = m.department_id
        group by  d.id
        """

        question_two_df = pd.read_sql_query(SQL, con)
        # Turns None Values in the id column to 0
        question_two_df['id'] = question_two_df['id'].fillna(0)
        # Stores values in id to be integers (previously stored as floats)
        question_two_df['id'] = question_two_df['id'].astype(int)
        # Replaces any remaining values with 'No Department Found'
        question_two_df = question_two_df.replace([None], 'No Department Found')

        # Loops through department dictionary to determine which departments are not present in the database.
        for key, value in department_dict['department_name'].items():
            department_id = int(key)
            department_name = value

            result = question_two_df.isin([department_name]).any().any()

            if str(result) == 'False':
                # Adds new a row into the dataframe to show which departments have zero students
                temp_df = {'id': department_id, 'department_name': department_name, 'students_per_department': 0}
                question_two_df = question_two_df.append(temp_df, ignore_index=True)

        # Stores dataframe output into a csv
        question_two_df.to_csv('question_two_output_student_per_dept.csv', index=False)

    StudentsPerMajor(con)
    StudentsPerDepartment(con)

def QuestionThree(con):

    """
    Takes a query from sqlite and inserts rows into csv file using the csv module.

    :param con:
    :return:

    """

    student_list = []

    headers = ['StudentID','FirstName','LastName','DOB','MajorID','MajorName']

    student_list.append(headers)

    SQL = """select s.id as 'student_id', first_name, last_name, dob, m.id AS 'Major_id', major_name  from student s
    left join student_major sm on s.id = sm.student_id
    left join major m on m.id = sm.major_id
    left join department d on d.id =  m.department_id"""

    cursor = con.cursor()
    cursor.execute(SQL)

    student_data = cursor.fetchall()

    #loops through output and stores into a list
    for row in student_data:
        student_list.append(row)

    #declares csv file and inputs list into file
    question_three_output = csv.writer(open('question_three.csv', 'w', newline=''), delimiter=',',quotechar='"', quoting=csv.QUOTE_MINIMAL)
    question_three_output.writerows(student_list)

def Question4(max_student, max_student_major, con):

    """
    Stores csv File into a dataframe using pandas and creates two additional dataframes in the same format as the student and student_major tables in the database

    :param max_student:
    :param max_student_major:
    :param con:
    :return:

    """

    def StudentTable(student_data_df, max_student):

        """
        :param student_data_df:
        :param max_student:
        :return old_id_to_new_id:

        """

        old_id_to_new_id = {}

        #Declares dataframe with columns needed for student table
        student_df = student_data_df[['StudentID','FirstName','LastName','DOB']].copy()
        #renames dataframe columns to the columns in the database
        student_df.rename(columns={"StudentID": "id", "FirstName": "first_name", 'LastName': 'last_name', 'DOB': 'dob'}, inplace = True)
        #drops any duplicate student records (since some have multiple majors)
        student_df.drop_duplicates(inplace = True)

        #Loops through indexes of dataframe and stores the new student_id into the id column of the dataframe
        for index in student_df.index:
            old_index = student_df.loc[index, 'id']
            student_df.loc[index, 'id']= max_student + 1

            old_id_to_new_id[old_index] = student_df.loc[index, 'id']

            max_student = max_student + 1

        #inserts dataframe in to the database
        student_df.to_sql(name='student', con=con, if_exists='append', index=False)

        con.commit()

        return old_id_to_new_id

    def StudentMajorsTable(student_data_df, max_student_major, old_id_to_new_id):
        """
        :param student_data_df:
        :param max_student_major:
        :param old_id_to_new_id:
        :return:
        """

        student_major_id_list = []
        index_to_delete = []

        # Declares dataframe with columns needed for student_major table
        student_major_df = student_data_df[['StudentID','MajorID']].copy()
        # renames dataframe columns to the columns in the database
        student_major_df.rename(columns={'StudentID': 'student_id', 'MajorID': 'major_id'}, inplace = True)

        #loops through indexs od dataframe
        for index in student_major_df.index:

            #updating student id
            old_student_id = student_major_df.loc[index, 'student_id']
            new_student_id = old_id_to_new_id[old_student_id]

            student_major_df.loc[index, 'student_id'] = new_student_id

            #checking if the majorid column is null and stores those indexes into index_to_delete
            if pd.isnull(student_major_df.loc[index, 'major_id']):
                index_to_delete.append(index)
                continue

            #creates a list of ids which will be inserted as a new column
            student_major_id_list.append(max_student_major + 1)
            max_student_major = max_student_major + 1

        #removes columns where the major_id was null aka student had no major
        student_major_df.drop(index_to_delete, inplace = True)
        #sets major id column to an integer (previously a float)
        student_major_df['major_id'] = student_major_df['major_id'].astype(int)
        #inserts id column into the dataframe using the student_major_id_list
        student_major_df.insert(0, "id", student_major_id_list, True)

        # inserts dataframe in to the database
        student_major_df.to_sql(name='student_major', con=con, if_exists='append', index=False)

        con.commit()

    #stores csv file into dataframe
    student_data_df = pd.read_csv('question_three.csv')

    old_id_to_new_id = StudentTable(student_data_df, max_student)
    StudentMajorsTable(student_data_df, max_student_major, old_id_to_new_id)

def main():
    con = sqlite3.connect("student_major.db")

    #Insert New Student Records from Yaml File Provided in newstudent.yaml

    max_student_major = GetMaxStudentMajor(con)
    max_student = GetMaxStudent(con)

    newstudents = yaml.load(open('.\\newstudent.yaml', 'r'), yaml.FullLoader)
    InsertNewStudentRecord(newstudents, max_student,con)

    # majors_dict = CreateMajorsDict(con)
    # department_dict = CreateDepartmentDict(con)
    #
    # #Question One Solution
    # QuestionOne(con)
    # #Question Two Solution
    # QuestionTwo(con, majors_dict,  department_dict)
    # #Question Three Soution
    # QuestionThree(con)
    # #Question Four Solution
    # Question4(max_student, max_student_major, con)


if __name__ == "__main__":
    main()