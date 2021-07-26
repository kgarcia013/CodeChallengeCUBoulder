@startuml
' hide the spot
hide circle

' avoid problems with angled crows feet
skinparam linetype ortho

entity student {
id: number
--
first_name : string
last_name : string
dob : datetime
}

entity student_major {
id: number
--
student_id : number
major_id : number
}

entity major {
id: number
--
major_name : string
department_id : string
}

entity department {
id: number
--
department_name: string
}

student ||-right-o{ student_major
student_major ||-right-|| major
major ||-right-|| department

@enduml