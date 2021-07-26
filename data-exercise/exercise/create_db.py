import sqlite3

db_name = "student_major.db"

create_sql = """
    drop table if exists department;
    drop table if exists major;
    drop table if exists student;
    drop table if exists student_major;
    
    create table department(
        id numeric primary key,
        department_name text
    );
    
    create table major(
        id numeric primary key,
        major_name text,
        department_id numeric,
        foreign key (department_id) references department (id)
    );
    
    create table student(
        id numeric primary key,
        first_name text,
        last_name text,
        dob date
    );
    
    create table student_major(
        id numeric primary key,
        student_id numeric,
        major_id numeric,
        foreign key (student_id) references student (id),
        foreign key (major_id) references major (id)
    );
    
    """

insert_sql = """
    insert into department (id, department_name) values (1, 'Engineering');
    insert into department (id, department_name) values (2, 'Natural Sciences');
    insert into department (id, department_name) values (3, 'Language Arts');
    
    insert into major (id, major_name, department_id) values (1, 'Biocomputation', 1);
    insert into major (id, major_name, department_id) values (2, 'Computer Science', 1);
    insert into major (id, major_name, department_id) values (3, 'Electrical Engineering', 1);
    insert into major (id, major_name, department_id) values (4, 'Geology', 2);
    insert into major (id, major_name, department_id) values (5, 'Biology', 2);
    insert into major (id, major_name, department_id) values (6, 'Chemistry', 2);
    insert into major (id, major_name, department_id) values (7, 'English', 3);
    insert into major (id, major_name, department_id) values (8, 'German', 3);
    insert into major (id, major_name, department_id) values (9, 'French', 3);

    insert into student (id, first_name, last_name, dob) values (1, 'Geetha', 'Manjunath', '1998-05-08');
    insert into student (id, first_name, last_name, dob) values (2, 'Kelsey', 'Hightower', '2001-11-20');  
    insert into student (id, first_name, last_name, dob) values (3, 'Grace', 'Hopper', '2002-03-04');
    insert into student (id, first_name, last_name, dob) values (4, 'Alan', 'Turing', '1999-01-22');
    insert into student (id, first_name, last_name, dob) values (5, 'Andrew', 'Ng', '2000-07-19');
    insert into student (id, first_name, last_name, dob) values (6, 'Corinna', 'Cortes', '1999-06-30');
    
    insert into student_major (id, student_id, major_id) values (1, 1, 1);
    insert into student_major (id, student_id, major_id) values (2, 1, 2);
    insert into student_major (id, student_id, major_id) values (3, 2, 3);
    insert into student_major (id, student_id, major_id) values (4, 3, 5);
    insert into student_major (id, student_id, major_id) values (5, 3, 2);
    """

with sqlite3.connect(db_name) as conn:
    cursor = conn.cursor()
    cursor.executescript(create_sql)
    cursor.executescript(insert_sql)
    conn.commit()