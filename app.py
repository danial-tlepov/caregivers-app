import os
from flask import Flask, render_template, request, redirect, url_for
from sqlalchemy import create_engine, text

app = Flask(__name__)

DATABASE_URI = 'postgresql://postgres:danthatcan@localhost:5432/caregivers_db'
engine = create_engine(DATABASE_URI)

def run_query(query, params=None):
    with engine.connect() as conn:
        result = conn.execute(text(query), params or {})
        conn.commit()
        return result

def fetch_all(query, params=None):
    with engine.connect() as conn:
        result = conn.execute(text(query), params or {})
        return result.fetchall()

def fetch_one(query, params=None):
    with engine.connect() as conn:
        result = conn.execute(text(query), params or {})
        return result.fetchone()

@app.route('/')
def index():
    return render_template('index.html')

# CRUD
@app.route('/users')
def users():
    # READ
    users_data = fetch_all('SELECT * FROM "USER" ORDER BY user_id ASC')
    return render_template('users.html', users=users_data)

@app.route('/user/add', methods=['GET', 'POST'])
def add_user():
    # CREATE
    if request.method == 'POST':
        user_id = request.form['user_id']
        email = request.form['email']
        given_name = request.form['given_name']
        surname = request.form['surname']
        password = request.form['password']
        
        sql = """
            INSERT INTO "USER" (user_id, email, given_name, surname, password)
            VALUES (:uid, :email, :name, :surname, :pass)
        """
        try:
            run_query(sql, {'uid': user_id, 'email': email, 'name': given_name, 'surname': surname, 'pass': password})
            return redirect(url_for('users'))
        except Exception as e:
            return f"Error: {e}"
            
    return render_template('user_form.html', action="Add", user={})

@app.route('/user/edit/<int:id>', methods=['GET', 'POST'])
def edit_user(id):
    # UPDATE
    if request.method == 'POST':
        email = request.form['email']
        given_name = request.form['given_name']
        surname = request.form['surname']
        
        sql = """
            UPDATE "USER" 
            SET email = :email, given_name = :name, surname = :surname 
            WHERE user_id = :uid
        """
        run_query(sql, {'email': email, 'name': given_name, 'surname': surname, 'uid': id})
        return redirect(url_for('users'))
    
    user = fetch_one('SELECT * FROM "USER" WHERE user_id = :uid', {'uid': id})
    return render_template('user_form.html', action="Edit", user=user)

@app.route('/user/delete/<int:id>')
def delete_user(id):
    # DELETE
    run_query('DELETE FROM "USER" WHERE user_id = :uid', {'uid': id})
    return redirect(url_for('users'))

if __name__ == '__main__':
    app.run(debug=True)

database_url = os.environ.get('DATABASE_URL')

if database_url:
    if database_url.startswith("postgres://"):
        database_url = database_url.replace("postgres://", "postgresql://", 1)
    DATABASE_URI = database_url
else:
    DATABASE_URI = 'postgresql://postgres:danthatcan@localhost:5432/caregivers_db'
    
engine = create_engine(DATABASE_URI)