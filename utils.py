from db_connect import get_db_connection
import psycopg2
from psycopg2 import sql

def get_author_list():
  conn = get_db_connection()
  cur = conn.cursor()

  try:
    # Execute the query
    cur.execute(sql.SQL("SELECT * FROM library.authors"))
    # Fetch all rows from the result
    authors = cur.fetchall()
    # Commit the transaction (if needed for your setup, sometimes this isn't necessary for SELECT statements)
    conn.commit()
    # Return the list of authors
    author_list = []
    for author in authors:
      author_list.append({"name": f"{author[1]}", "id": f"{author[0]}"})
    return author_list
  except psycopg2.Error as e:
    print(f"An error occurred while fetching authors: {e}")
    conn.rollback()
    return []  # You might want to return an empty list or None in case of an error
  finally:
    # Ensure the cursor and connection are closed
    cur.close()
    conn.close()

def get_illustrator_list():
  conn = get_db_connection()
  cur = conn.cursor()

  try:
    # Execute the query
    cur.execute(sql.SQL("SELECT * FROM library.illustrators"))
    # Fetch all rows from the result
    illustrators = cur.fetchall()
    # Commit the transaction (if needed for your setup, sometimes this isn't necessary for SELECT statements)
    conn.commit()
    # Return the list of authors
    illustrator_list = []
    for illustrator in illustrators:
      illustrator_list.append({"name": f"{illustrator[1]}", "id": f"{illustrator[0]}"})
    return illustrator_list
  except psycopg2.Error as e:
    print(f"An error occurred while fetching illustrators: {e}")
    conn.rollback()
    return []  # You might want to return an empty list or None in case of an error
  finally:
    # Ensure the cursor and connection are closed
    cur.close()
    conn.close()

def get_types_list():
  conn = get_db_connection()
  cur = conn.cursor()

  try:
    # Execute the query
    cur.execute(sql.SQL("SELECT * FROM library.types"))
    # Fetch all rows from the result
    types = cur.fetchall()
    # Commit the transaction (if needed for your setup, sometimes this isn't necessary for SELECT statements)
    conn.commit()
    # Return the list of authors
    type_list = []
    for type in types:
      type_list.append({"name": f"{type[1]}", "id": f"{type[0]}"})
    return type_list
  except psycopg2.Error as e:
    print(f"An error occurred while fetching types: {e}")
    conn.rollback()
    return []  # You might want to return an empty list or None in case of an error
  finally:
    # Ensure the cursor and connection are closed
    cur.close()
    conn.close()

def get_category_list():
  conn = get_db_connection()
  cur = conn.cursor()

  try:
    # Execute the query
    cur.execute(sql.SQL("SELECT * FROM library.categories"))
    # Fetch all rows from the result
    categories = cur.fetchall()
    # Commit the transaction (if needed for your setup, sometimes this isn't necessary for SELECT statements)
    conn.commit()
    # Return the list of authors
    category_list = []
    for category in categories:
      category_list.append({"name": f"{category[1]}", "id": f"{category[0]}"})
    return category_list
  except psycopg2.Error as e:
    print(f"An error occurred while fetching categories: {e}")
    conn.rollback()
    return []  # You might want to return an empty list or None in case of an error
  finally:
    # Ensure the cursor and connection are closed
    cur.close()
    conn.close()

def get_subjects_list():
  conn = get_db_connection()
  cur = conn.cursor()

  try:
    # Execute the query
    cur.execute(sql.SQL("SELECT * FROM library.subjects"))
    # Fetch all rows from the result
    subjects = cur.fetchall()
    # Commit the transaction (if needed for your setup, sometimes this isn't necessary for SELECT statements)
    conn.commit()
    # Return the list of authors
    subject_list = []
    for subject in subjects:
      subject_list.append({"name": f"{subject[1]}", "id": f"{subject[0]}"})
    return subject_list
  except psycopg2.Error as e:
    print(f"An error occurred while fetching subjects: {e}")
    conn.rollback()
    return []  # You might want to return an empty list or None in case of an error
  finally:
    # Ensure the cursor and connection are closed
    cur.close()
    conn.close()

def check_list(author, author_list):
  if len(author_list) > 0:
    for person in author_list:
      if author == person["name"]:
        return person
  return False

def split_authors(author_string):
  # Check if there's a comma followed by space in the string
  if ", " in author_string:
      # Split the string by ", " and return as a list of strings
      return author_string.split(", ")
  else:
      # If no comma and space are found, return the author_string in an array for further processing
      return [author_string]