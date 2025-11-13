from flask import Flask, render_template, url_for, request, redirect, session, jsonify
import mysql.connector
from groq import Groq
from dotenv import load_dotenv
from markupsafe import Markup
import markdown
import os
import traceback
import re

load_dotenv()

app = Flask(__name__)

app.secret_key = os.getenv("APP_SECRET_KEY")
client = Groq(api_key=os.getenv("GROQ_API_KEY"))

session_history = []


def get_db_connection():
    db = session.get("active_db", None)

    return mysql.connector.connect(
        host="localhost",
        user=session.get("db_user"),
        password=session.get("db_password"),
        database=db
    )


SQL_KEYWORDS = [
    "SELECT", "INSERT", "UPDATE", "DELETE", "CREATE", "DROP", "ALTER",
    "WHERE", "FROM", "JOIN", "INNER", "LEFT", "RIGHT", "ON", "GROUP BY",
    "ORDER BY", "LIMIT", "VALUES", "SET", "USE"
]

def highlight_sql_keywords(text):
    def replacer(match):
        return f'<span class="sql-keyword">{match.group(0)}</span>'
    
    pattern = r'\b(' + '|'.join(SQL_KEYWORDS) + r')\b'
    return re.sub(pattern, replacer, text, flags=re.IGNORECASE)


@app.route("/editor")
def editor():
    if not session.get('connected'):
        return redirect(url_for('home'))

    conn = get_db_connection()
    cursor = conn.cursor()
    
    db_schema_rich = {}
    databases = []

    try:
        cursor.execute("SHOW DATABASES")
        hidden_dbs = {'mysql', 'information_schema', 'performance_schema', 'sys'}
        databases = [row[0] for row in cursor.fetchall() if row[0] not in hidden_dbs]

        if not databases:
            return render_template("editor.html", databases=[], db_schema={})

        # Add databases to the schema with their type
        for db in databases:
            db_schema_rich[db] = [{"name": db, "type": "database"}]

        # Get all tables and columns
        query = f"""
            SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME
            FROM information_schema.columns
            WHERE TABLE_SCHEMA IN ({', '.join(['%s'] * len(databases))})
            ORDER BY TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION;
        """
        cursor.execute(query, databases)
        all_columns = cursor.fetchall()

        # Process into the rich schema structure
        for row in all_columns:
            db_name, table_name, column_name = row[0], row[1], row[2]
            
            # Add simple table name
            if table_name not in db_schema_rich:
                db_schema_rich[table_name] = [{"name": table_name, "type": "table"}]
            
            # Add fully qualified table name
            qualified_table_name = f"{db_name}.{table_name}"
            if qualified_table_name not in db_schema_rich:
                db_schema_rich[qualified_table_name] = [{"name": qualified_table_name, "type": "table"}]
            
            # Add columns to both
            db_schema_rich[table_name].append({"name": column_name, "type": "column"})
            db_schema_rich[qualified_table_name].append({"name": column_name, "type": "column"})

    except Exception:
        print("--- AN EXCEPTION OCCURRED WHILE FETCHING RICH SCHEMA ---")
        traceback.print_exc()
        db_schema_rich = {}
        databases = []

    db_schema = db_schema_rich
    
    return render_template("editor.html", databases=databases, db_schema=db_schema)


@app.route("/home")
def home():
    session.clear()
    session['connected'] = False
    return render_template("index.html")


@app.route('/connect', methods=['POST'])
def connect():
    data = request.get_json()
    session['connected'] = False

    try:
        # Try to establish connection
        connection = mysql.connector.connect(
            host='localhost',
            user=data.get('username'),
            password=data.get('password'),
        )

        # If connection is successful, store credentials in session
        session['db_user'] = data['username']
        session['db_password'] = data['password']
        session['connected'] = True

        return jsonify({"success": True, "message": "Connected successfully"}), 200

    except mysql.connector.Error as err:
        return jsonify({"success": False, "message": str(err)}), 400

    finally:
        if 'connection' in locals() and connection.is_connected():
            connection.close()



@app.route('/get_tables', methods=['POST'])
def get_tables():
    if session.get('connected') != True:
        return redirect(url_for('home'))
    
    data = request.get_json()
    db_name = data.get('db_name')
    username = session.get('db_user')
    password = session.get('db_password')

    if not db_name or not username or not password:
        return jsonify({"success": False, "tables": [], "message": "Missing data"})

    try:
        connection = mysql.connector.connect(
            host='localhost',
            user=username,
            password=password,
            database=db_name
        )
        cursor = connection.cursor()
        cursor.execute("SHOW TABLES")
        tables = [t[0] for t in cursor.fetchall()]
        return jsonify({"success": True, "tables": tables})
    except mysql.connector.Error as err:
        return jsonify({"success": False, "tables": [], "message": str(err)})
    finally:
        if 'connection' in locals() and connection.is_connected():
            connection.close()


@app.route('/table-click', methods=['POST'])
def table_click():
    if session.get('connected') != True:
        return redirect(url_for('home'))
    
    data = request.get_json()
    db_name = data.get('db_name')
    username = session.get('db_user')
    password = session.get('db_password')
    table_name = data.get('table_name')

    if not db_name or not username or not password or not table_name:
        return jsonify({"success": False, "tables": [], "message": "Missing data"})

    try:
        connection = mysql.connector.connect(
            host='localhost',
            user=username,
            password=password,
            database=db_name
        )
        cursor = connection.cursor()
        cursor.execute(f"SELECT * FROM {table_name}")
        rows = cursor.fetchall()

        # Fetch column names
        columns = [col[0] for col in cursor.description]

        cursor.close()

        return jsonify({
            "success": True,
            "columns": columns,
            "rows": rows
        })

    except mysql.connector.Error as err:
        return jsonify({"success": False, "columns": [], "rows": [], "message": str(err)})
    finally:
        if 'connection' in locals() and connection.is_connected():
            connection.close()




@app.route("/chat", methods=["POST"])
def chat():
    user_msg = request.json.get("message", "")
    editor_sql = request.json.get("sql_code", "").strip()

    intent = client.chat.completions.create(
        model="llama-3.3-70b-versatile",
        messages=[
            {
                "role": "system",
                "content": (
                    "You are an intent classifier. Reply only YES or NO.\n"
                    "Does the user asks or wants you read the screen/editor , database queries, fixing SQL, errors, debugging, or checking correctness?"
                )
            },
            {"role": "user", "content": user_msg}
        ]
    )

    wants_sql = "yes" in intent.choices[0].message.content.lower()

    if wants_sql and editor_sql == "":
        return jsonify({"reply": "Your SQL editor is empty. Please write some SQL first."})

    if wants_sql:
        system_prompt = (
            "You are an SQL expert. Analyze the SQL from the editor (read the screen) "
            "Detect syntax errors, explain briefly, and provide corrected queries. "
            "If correct, say it is correct. Use code blocks."
        )
        final_message = f"Please analyze this SQL:\n```sql\n{editor_sql}\n```"
    else:
        system_prompt = "You are a friendly chatbot. Respond like a helpful human. Keep replies short."
        final_message = user_msg

    if not wants_sql:
        session_history.append({"role": "user", "content": user_msg})
        if len(session_history) > 10:
            session_history.pop(0)

    completion = client.chat.completions.create(
        model="llama-3.3-70b-versatile",
        messages=[
            {"role": "system", "content": system_prompt},
            *session_history,
            {"role": "user", "content": final_message}
        ]
    )

    reply = completion.choices[0].message.content

    if not wants_sql:
        session_history.append({"role": "assistant", "content": reply})

    reply_html = markdown.markdown(reply, extensions=['fenced_code', 'tables'])

    if wants_sql:
        reply_html = re.sub(
            r'\b(SELECT|FROM|WHERE|INSERT|INTO|VALUES|UPDATE|DELETE|CREATE|TABLE|ALTER|DROP|JOIN|ORDER BY|GROUP BY|SET)\b',
            r"<span style='color:#ff9800;font-weight:bold'>\1</span>",
            reply_html,
            flags=re.IGNORECASE
        )

    return jsonify({"reply": Markup(reply_html)})


@app.route("/run-sql", methods=["POST"])
def run_sql():
    data = request.get_json()
    full_query = data.get("query")

    commands = [c.strip() for c in full_query.split(";") if c.strip()]

    last_output = None

    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        for cmd in commands:
            cmd_lower = cmd.lower()


            if cmd_lower.startswith("use "):
                db_name = cmd.split()[1]

                db_name = db_name.replace(";", "").replace("`", "")

                conn = mysql.connector.connect(
                    host="localhost",
                    user=session.get("db_user"),
                    password=session.get("db_password")
                )

                c = conn.cursor()
                c.execute("SHOW DATABASES")
                db_list = [d[0] for d in c.fetchall()]
                conn.close()

                if db_name not in db_list:
                    return jsonify({"error": f"Unknown database '{db_name}'"}), 400

                session["active_db"] = db_name

                last_output = {"message": f"Database changed to `{db_name}`"}

            conn = get_db_connection()
            cursor = conn.cursor()
            cursor.execute(cmd)

            # store output of last command that produces rows
            if cursor.description:
                rows = cursor.fetchall()
                columns = [desc[0] for desc in cursor.description]
                last_output = {"rows": rows, "columns": columns}
            else:
                conn.commit()
                last_output = {
                    "rows": [],
                    "columns": [],
                    "message": "Query executed successfully."
                }
            cursor.close()
            conn.close()

        if last_output is None:
            return jsonify({"error": "No valid SQL commands found."}), 400

        return jsonify(last_output)

    except Exception as e:
        return jsonify({"error": str(e)}), 400
    
    finally:
        try:
            cursor.close()
            conn.close()
        except:
            pass


@app.route('/logout')
def logout():
    session.clear()
    return redirect(url_for('home'))


if __name__ == "__main__":
    app.run(debug=True)