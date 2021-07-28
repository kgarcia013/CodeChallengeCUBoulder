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


def CreateMajorsDict(con):
    SQL = """select id, major_name from major"""

    majors_df = pd.read_sql_query(SQL, con)
    majors_df = majors_df.set_index('id')
    majors_dict = majors_df.to_dict()

    return majors_dict


def CreateDepartmentDict(con):
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

    def StudentsPerMajor(con):
        SQL = """select m.id, m.major_name, count(*) as 'students_per_major' from student s
        left join student_major sm on s.id = sm.student_id
        left join major m on m.id = sm.major_id
        group by  m.id"""

        question_two_df = pd.read_sql_query(SQL, con)
        question_two_df['id'] = question_two_df['id'].fillna(0)
        question_two_df['id'] = question_two_df['id'].astype(int)
        question_two_df = question_two_df.replace([None], 'Students with No Major')

        for key, value in majors_dict['major_name'].items():
            major_id = int(key)
            major_name = value

            result = question_two_df.isin([major_name]).any().any()

            print(str(result))

            if str(result) == 'False':
                temp_df = {'id': major_id, 'major_name': major_name, 'students_per_major': 0}
                question_two_df = question_two_df.append(temp_df, ignore_index=True)


        question_two_df.to_csv('question_two_output_student_per_major.csv', index=False)

    def StudentsPerDepartment(con):

        SQL = """select d.id, d.department_name, count(*) as 'students_per_department' from student s
        left join student_major sm on s.id = sm.student_id
        left join major m on m.id = sm.major_id
        left join department d on d.id = m.department_id
        group by  d.id
        """

        question_two_df = pd.read_sql_query(SQL, con)
        question_two_df['id'] = question_two_df['id'].fillna(0)
        question_two_df['id'] = question_two_df['id'].astype(int)
        question_two_df = question_two_df.replace([None], 'No Department Found')

        for key, value in department_dict['department_name'].items():
            department_id = int(key)
            department_name = value

            result = question_two_df.isin([department_name]).any().any()

            print(str(result))

            if str(result) == 'False':
                temp_df = {'id': department_id, 'department_name': department_name, 'students_per_department': 0}
                question_two_df = question_two_df.append(temp_df, ignore_index=True)


        question_two_df.to_csv('question_two_output_student_per_dept.csv', index=False)

    StudentsPerMajor(con)
    StudentsPerDepartment(con)


def main():
    con = sqlite3.connect("student_major.db")

    """
    Insert New Student Records from Yaml File Provided in newstudent.yaml
    """
    # newstudents = yaml.load(open('.\\newstudent.yaml', 'r'))
    # InsertNewStudentRecord(newstudents, con)

    majors_dict = CreateMajorsDict(con)
    department_dict = CreateDepartmentDict(con)

    QuestionOne(con)
    QuestionTwo(con, majors_dict,  department_dict)


if __name__ == "__main__":
    main()