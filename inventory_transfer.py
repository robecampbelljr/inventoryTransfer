import openpyxl
import psycopg2
from psycopg2 import sql
from db_connect import get_db_connection
from utils import get_author_list, get_illustrator_list, get_types_list, check_list, get_category_list, get_subjects_list, split_authors

wb = openpyxl.load_workbook('campbell_library_inventory.xlsx')
sheet = wb['Sheet1']

# Connect to the database
conn = get_db_connection()

cur = conn.cursor()

insert_author_query = sql.SQL("INSERT INTO library.authors (author_name) VALUES (%s) RETURNING author_id;")
insert_illustrator_query = sql.SQL("INSERT INTO library.illustrators (illustrator_name) VALUES (%s) RETURNING illustrator_id;")
insert_bookType_query = sql.SQL("INSERT INTO library.types (type_name) VALUES (%s) RETURNING type_id;")
insert_category_query = sql.SQL("INSERT INTO library.categories (category_name) VALUES (%s) RETURNING category_id;")
insert_subject_query = sql.SQL("INSERT INTO library.subjects (subject_name) VALUES (%s) RETURNING subject_id;")
insert_book_query = sql.SQL("INSERT INTO library.books (title, illustrator_id, type_id, category_id, subject_id, weh_rotation_month) VALUES (%s, %s, %s, %s, %s, %s) RETURNING book_id;")
insert_book_authors = sql.SQL("INSERT INTO library.book_authors (book_id, author_id) VALUES (%s, %s)")

default_value = "N/A"

author_list = get_author_list()
illustrator_list = get_illustrator_list()
bookType_list = get_types_list()
category_list = get_category_list()
subject_list = get_subjects_list()

# Iterate through each row in the spreadsheet
for row in sheet.iter_rows(min_row=2, max_col=7, values_only=True):
    title, author, illustrator, bookType, category, subject, weh = [default_value if cell is None else cell for cell in row]

    book_id = 0
    author_id = 0
    illustrator_id = 0
    bookType_id = 0
    category_id = 0
    subject_id = 0

    try:
        illustrator_present = check_list(illustrator, illustrator_list)
        if not illustrator_present:
            cur.execute(insert_illustrator_query, (illustrator,))
            illustrator_id = cur.fetchone()[0]
            conn.commit()
            illustrator_list.append({"name": f"{illustrator}", "id": f"{illustrator_id}"})
        elif illustrator_present:
            illustrator_id = illustrator_present["id"]
    except psycopg2.Error as e:
        print(f"An error occurred while inserting: {e}")
        conn.rollback()  # Rollback on error

    try:
        bookType_present = check_list(bookType, bookType_list)
        if not bookType_present:
            cur.execute(insert_bookType_query, (bookType,))
            bookType_id = cur.fetchone()[0]
            conn.commit()
            bookType_list.append({"name": f"{bookType}", "id": f"{bookType_id}"})
        elif bookType_present:
            bookType_id = bookType_present["id"]
    except psycopg2.Error as e:
        print(f"An error occurred while inserting: {e}")
        conn.rollback()  # Rollback on error

    try:
        category_present = check_list(category, category_list)
        if not category_present:
            cur.execute(insert_category_query, (category,))
            category_id = cur.fetchone()[0]
            conn.commit()
            category_list.append({"name": f"{category}", "id": f"{category_id}"})
        elif category_present:
            category_id = category_present["id"]
    except psycopg2.Error as e:
        print(f"An error occurred while inserting: {e}")
        conn.rollback()  # Rollback on error

    try:
        subject_present = check_list(subject, subject_list)
        if not subject_present:
            cur.execute(insert_subject_query, (subject,))
            subject_id = cur.fetchone()[0]
            conn.commit()
            subject_list.append({"name": f"{subject}", "id": f"{subject_id}"})
        elif subject_present:
            subject_id = subject_present["id"]
    except psycopg2.Error as e:
        print(f"An error occurred while inserting: {e}")
        conn.rollback()  # Rollback on error

    try:
        cur.execute(insert_book_query, (title, illustrator_id, bookType_id, category_id, subject_id, weh))
        book_id = cur.fetchone()[0]
        conn.commit()  # Commit after each insert to keep transactions small
    except psycopg2.Error as e:
        print(f"An error occurred while inserting: {e}")
        conn.rollback()  # Rollback on error

    try:
        author_present = check_list(author, author_list)
        if not author_present:
            multiple_author_check = split_authors(author)
            for new_author in multiple_author_check:
                cur.execute(insert_author_query, (new_author,))
                author_id = cur.fetchone()[0]
                conn.commit()
                cur.execute(insert_book_authors, (book_id, author_id))
                conn.commit()
                author_list.append({"name": f"{new_author}", "id": f"{author_id}"})
        elif author_present:
            author_id = author_present["id"]
    except psycopg2.Error as e:
        print(f"An error occurred while inserting: {e}")
        conn.rollback()  # Rollback on error

# If you want to ensure all changes are committed at the end
conn.commit()

cur.close()
conn.close()

print("Data insertion process completed.")