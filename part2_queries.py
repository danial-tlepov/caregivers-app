from sqlalchemy import create_engine, text

DATABASE_URI = 'postgresql://postgres:danthatcan@localhost:5432/caregivers_db'

engine = create_engine(DATABASE_URI)

def run_query(query, params=None):
    """Helper function to execute a query and commit changes."""
    with engine.connect() as connection:
        # We use text() to declare the SQL string
        result = connection.execute(text(query), params or {})
        connection.commit()
        return result

def run_select(query, params=None):
    """Helper function to execute a SELECT query and print results."""
    with engine.connect() as connection:
        result = connection.execute(text(query), params or {})
        rows = result.fetchall()
        print(f"\n--- Query Result ({len(rows)} rows) ---")
        if not rows:
            print("No results found.")
        for row in rows:
            print(row)
        return rows

# INSERT DATA (At least 10 rows per table)
def insert_data():
    print("Inserting data...")
    
    users_sql = """
    INSERT INTO "USER" (user_id, email, given_name, surname, city, phone_number, profile_description, password) VALUES
    (1, 'c1@test.com', 'Alice', 'Smith', 'Astana', '111222333', 'Exp nurse', 'pass1'),
    (2, 'c2@test.com', 'Bob', 'Jones', 'Almaty', '222333444', 'Good with kids', 'pass2'),
    (3, 'c3@test.com', 'Charlie', 'Brown', 'Astana', '333444555', 'Student', 'pass3'),
    (4, 'c4@test.com', 'Diana', 'Prince', 'Shymkent', '444555666', 'Pro nanny', 'pass4'),
    (5, 'c5@test.com', 'Evan', 'Wright', 'Almaty', '555666777', 'Elderly specialist', 'pass5'),
    (6, 'm1@test.com', 'Arman', 'Armanov', 'Astana', '87010000001', 'Need help', 'pass6'),
    (7, 'm2@test.com', 'Amina', 'Aminova', 'Almaty', '87010000002', 'Working mom', 'pass7'),
    (8, 'm3@test.com', 'Berik', 'Serikov', 'Astana', '87010000003', 'Busy dad', 'pass8'),
    (9, 'm4@test.com', 'Dina', 'Dina', 'Astana', '87010000004', 'Grandma needs help', 'pass9'),
    (10, 'm5@test.com', 'Elena', 'Popova', 'Almaty', '87010000005', 'Twins mom', 'pass10'),
    (11, 'm6@test.com', 'Fara', 'Farov', 'Shymkent', '87010000006', 'Single dad', 'pass11'),
    (12, 'm7@test.com', 'Gani', 'Gani', 'Astana', '87010000007', 'Busy', 'pass12'),
    (13, 'm8@test.com', 'Hana', 'Montana', 'Almaty', '87010000008', 'Singer', 'pass13'),
    (14, 'm9@test.com', 'Ivan', 'Ivanov', 'Astana', '87010000009', 'Teacher', 'pass14'),
    (15, 'm10@test.com', 'Julia', 'Roberts', 'Almaty', '87010000010', 'Actress', 'pass15')
    ON CONFLICT (user_id) DO NOTHING;
    """
    run_query(users_sql)

    # Insert CAREGIVERS
    caregivers_sql = """
    INSERT INTO CAREGIVER (caregiver_user_id, photo, gender, caregiving_type, hourly_rate) VALUES
    (1, 'photo1.jpg', 'Female', 'caregiver for elderly', 8.00),
    (2, 'photo2.jpg', 'Male', 'babysitter', 15.00),
    (3, 'photo3.jpg', 'Male', 'playmate for children', 9.50),
    (4, 'photo4.jpg', 'Female', 'babysitter', 12.00),
    (5, 'photo5.jpg', 'Male', 'caregiver for elderly', 20.00)
    ON CONFLICT (caregiver_user_id) DO NOTHING;
    """
    run_query(caregivers_sql)

    # Insert MEMBERS
    members_sql = """
    INSERT INTO MEMBER (member_user_id, house_rules, dependent_description) VALUES
    (6, 'No smoking', '2 kids'),
    (7, 'Shoes off', '1 baby'),
    (8, 'Clean up after', '3 kids'),
    (9, 'No pets', 'Elderly father'),
    (10, 'Quiet after 9pm', 'Twins'),
    (11, 'Vegetarian food only', 'Son'),
    (12, 'None', 'Daughter'),
    (13, 'No loud music', 'Baby'),
    (14, 'Be on time', 'Toddler'),
    (15, 'Mask required', 'Newborn')
    ON CONFLICT (member_user_id) DO NOTHING;
    """
    run_query(members_sql)

    # Insert ADDRESSES
    address_sql = """
    INSERT INTO ADDRESS (member_user_id, house_number, street, town) VALUES
    (6, '10', 'Mangilik El', 'Astana'),
    (7, '20', 'Abay', 'Almaty'),
    (8, '55', 'Kabanbay Batyr', 'Astana'),
    (9, '12', 'Republic', 'Astana'),
    (10, '5', 'Dostyk', 'Almaty'),
    (11, '3', 'Tauke Khan', 'Shymkent'),
    (12, '99', 'Turan', 'Astana'),
    (13, '44', 'Al-Farabi', 'Almaty'),
    (14, '77', 'Kunaev', 'Astana'),
    (15, '88', 'Gogol', 'Almaty')
    ON CONFLICT (member_user_id) DO NOTHING;
    """
    run_query(address_sql)

    # Insert JOBS
    jobs_sql = """
    INSERT INTO JOB (job_id, member_user_id, required_caregiving_type, other_requirements, date_posted) VALUES
    (1, 7, 'babysitter', 'Must be kind', '2025-10-01'),
    (2, 7, 'babysitter', 'Weekend availability', '2025-10-02'),
    (3, 6, 'caregiver for elderly', 'Must be soft-spoken', '2025-10-03'),
    (4, 8, 'playmate for children', 'Energetic', '2025-10-04'),
    (5, 9, 'caregiver for elderly', 'Medical experience', '2025-10-05'),
    (6, 10, 'babysitter', 'English speaker', '2025-10-06'),
    (7, 11, 'playmate for children', 'Funny', '2025-10-07'),
    (8, 12, 'babysitter', 'Car required', '2025-10-08'),
    (9, 13, 'caregiver for elderly', 'Night shift', '2025-10-09'),
    (10, 14, 'babysitter', 'Urgent', '2025-10-10')
    ON CONFLICT (job_id) DO NOTHING;
    """
    run_query(jobs_sql)

    # Insert JOB APPLICATIONS
    apps_sql = """
    INSERT INTO JOB_APPLICATION (caregiver_user_id, job_id, date_applied) VALUES
    (1, 1, '2025-10-02'),
    (2, 1, '2025-10-02'),
    (3, 1, '2025-10-03'),
    (1, 3, '2025-10-04'),
    (2, 4, '2025-10-05'),
    (4, 5, '2025-10-06'),
    (5, 6, '2025-10-07'),
    (3, 7, '2025-10-08'),
    (1, 8, '2025-10-09'),
    (2, 9, '2025-10-10')
    ON CONFLICT (caregiver_user_id, job_id) DO NOTHING;
    """
    run_query(apps_sql)

    # Insert APPOINTMENTS
    appt_sql = """
    INSERT INTO APPOINTMENT (appointment_id, caregiver_user_id, member_user_id, appointment_date, appointment_time, work_hours, status) VALUES
    (1, 1, 6, '2025-11-01', '09:00:00', 5, 'Accepted'),
    (2, 2, 7, '2025-11-02', '10:00:00', 4, 'Accepted'),
    (3, 3, 8, '2025-11-03', '14:00:00', 3, 'Pending'),
    (4, 4, 9, '2025-11-04', '08:00:00', 8, 'Accepted'),
    (5, 5, 10, '2025-11-05', '12:00:00', 2, 'Declined'),
    (6, 1, 11, '2025-11-06', '09:00:00', 5, 'Accepted'),
    (7, 2, 12, '2025-11-07', '18:00:00', 4, 'Accepted'),
    (8, 3, 13, '2025-11-08', '10:00:00', 3, 'Pending'),
    (9, 4, 14, '2025-11-09', '11:00:00', 6, 'Accepted'),
    (10, 5, 15, '2025-11-10', '15:00:00', 2, 'Accepted')
    ON CONFLICT (appointment_id) DO NOTHING;
    """
    run_query(appt_sql)
    print("Data insertion complete.")



# ASSIGNMENT QUERIES
def execute_assignment_queries():
    
    # Update SQL Statements
    print("\n>>> 3.1 Update Phone of Arman Armanov")
    run_query("""
        UPDATE "USER" 
        SET phone_number = '+77773414141' 
        WHERE given_name = 'Arman' AND surname = 'Armanov';
    """)

    print("\n>>> 3.2 Update Hourly Rate (Commission Fee)")
    run_query("""
        UPDATE CAREGIVER
        SET hourly_rate = CASE
            WHEN hourly_rate < 10 THEN hourly_rate + 0.3
            ELSE hourly_rate * 1.10
        END;
    """)

    # Delete SQL Statements
    print("\n>>> 4.1 Delete jobs posted by Amina Aminova")
    run_query("""
        DELETE FROM JOB 
        WHERE member_user_id IN (
            SELECT user_id FROM "USER" WHERE given_name = 'Amina' AND surname = 'Aminova'
        );
    """)

    print("\n>>> 4.2 Delete members living on Kabanbay Batyr")
    run_query("""
        DELETE FROM MEMBER 
        WHERE member_user_id IN (
            SELECT member_user_id FROM ADDRESS WHERE street = 'Kabanbay Batyr'
        );
    """)

    print("\n>>> 5.1 Caregiver and Member names for Accepted appointments")
    run_select("""
        SELECT C_User.given_name AS Caregiver, M_User.given_name AS Member
        FROM APPOINTMENT A
        JOIN "USER" C_User ON A.caregiver_user_id = C_User.user_id
        JOIN "USER" M_User ON A.member_user_id = M_User.user_id
        WHERE A.status = 'Accepted';
    """)

    print("\n>>> 5.2 Job IDs with 'soft-spoken' in requirements")
    run_select("""
        SELECT job_id, other_requirements FROM JOB 
        WHERE other_requirements LIKE '%soft-spoken%';
    """)

    print("\n>>> 5.3 Work hours of all babysitter positions (from Caregivers)")
    run_select("""
        SELECT A.work_hours 
        FROM APPOINTMENT A
        JOIN CAREGIVER C ON A.caregiver_user_id = C.caregiver_user_id
        WHERE C.caregiving_type = 'babysitter';
    """)

    print("\n>>> 5.4 Members looking for Elderly Care in Astana with 'No pets' rule")
    run_select("""
        SELECT U.given_name, U.surname 
        FROM MEMBER M
        JOIN "USER" U ON M.member_user_id = U.user_id
        JOIN ADDRESS A ON M.member_user_id = A.member_user_id
        JOIN JOB J ON M.member_user_id = J.member_user_id
        WHERE J.required_caregiving_type = 'caregiver for elderly'
          AND A.town = 'Astana'
          AND M.house_rules LIKE '%No pets%';
    """)

    print("\n>>> 6.1 Count applicants for each job posted by a member")
    run_select("""
        SELECT J.job_id, COUNT(JA.caregiver_user_id) as applicant_count
        FROM JOB J
        LEFT JOIN JOB_APPLICATION JA ON J.job_id = JA.job_id
        GROUP BY J.job_id;
    """)

    print("\n>>> 6.2 Total hours spent by caregivers for all accepted appointments")
    run_select("""
        SELECT caregiver_user_id, SUM(work_hours) as total_hours
        FROM APPOINTMENT
        WHERE status = 'Accepted'
        GROUP BY caregiver_user_id;
    """)

    print("\n>>> 6.3 Average pay of caregivers based on accepted appointments")
    run_select("""
        SELECT AVG(C.hourly_rate) as average_rate
        FROM CAREGIVER C
        JOIN APPOINTMENT A ON C.caregiver_user_id = A.caregiver_user_id
        WHERE A.status = 'Accepted';
    """)

    print("\n>>> 6.4 Caregivers who earn above average (based on accepted appointments)")
    run_select("""
        SELECT C.caregiver_user_id, C.hourly_rate
        FROM CAREGIVER C
        JOIN APPOINTMENT A ON C.caregiver_user_id = A.caregiver_user_id
        WHERE A.status = 'Accepted'
        GROUP BY C.caregiver_user_id, C.hourly_rate
        HAVING C.hourly_rate > (
            SELECT AVG(C2.hourly_rate)
            FROM CAREGIVER C2
            JOIN APPOINTMENT A2 ON C2.caregiver_user_id = A2.caregiver_user_id
            WHERE A2.status = 'Accepted'
        );
    """)

    # Query with Derived Attribute
    print("\n>>> 7. Total cost to pay for a caregiver for all accepted appointments")
    run_select("""
        SELECT A.appointment_id, (C.hourly_rate * A.work_hours) as total_cost
        FROM APPOINTMENT A
        JOIN CAREGIVER C ON A.caregiver_user_id = C.caregiver_user_id
        WHERE A.status = 'Accepted';
    """)

    # View Operation
    print("\n>>> 8. Create and View all job applications")
    run_query("DROP VIEW IF EXISTS application_view;")
    run_query("""
        CREATE VIEW application_view AS
        SELECT JA.job_id, U.given_name AS Applicant_Name, JA.date_applied
        FROM JOB_APPLICATION JA
        JOIN "USER" U ON JA.caregiver_user_id = U.user_id;
    """)
    run_select("SELECT * FROM application_view;")

if __name__ == "__main__":
    insert_data()
    execute_assignment_queries()