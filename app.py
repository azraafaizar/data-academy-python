from fastapi import FastAPI
from pydantic import BaseModel
import pandas as pd
import psycopg2

app = FastAPI()

connection = psycopg2.connect(database="postgres", user="postgres.qollpkburpqlzxkvliyu", password="Chowmein.08011968", host="aws-0-eu-west-2.pooler.supabase.com", port=5432)

def execute_query(sql: str):
    try:
        cursor = connection.cursor()
        print(sql)
        cursor.execute(sql)

        # Fetch all rows from database
        record = cursor.fetchall()
        return record
    except Exception as e:
        print(e)
    finally:
        if cursor:
            cursor.close()

def handle_get_customer(name: str): 
    db_query = f"SELECT * FROM customerorders.customer WHERE customer_name = '{name}'"
    data = execute_query(db_query)
    return data

def get_all_products():
    db_query = f"SELECT * FROM customerorders.product"
    data = execute_query(db_query)
    print(data)
    return data

def get_single_products(product_id: int):
    db_query = f"SELECT * FROM customerorders.product WHERE product_id = '{product_id}'"
    data = execute_query(db_query)
    if not data:
        return {"error": "Product not found"}
    return data

def get_single_customer(customer_id):
    db_query = f"SELECT * FROM customerorders.customer WHERE customer_id = '{customer_id}'"
    data = execute_query(db_query)
    print(data)
    return data

def get_all_customers():
    sql = "SELECT customer_id, customer_name, email, city FROM customerorders.customer"
    data = execute_query(sql)
    return data

class CustomerCreate(BaseModel):
    customer_name: str
    email: str
    phone_number: str
    address_line_1: str
    city: str

def add_customer(customer: CustomerCreate):
    sql = """
        INSERT INTO customerorders.customer 
        (customer_name, email, phone_number, address_line_1, city)
        VALUES (%s, %s, %s, %s, %s)
        RETURNING customer_id, customer_name, email, phone_number, address_line_1, city
    """
    try:
        cursor = connection.cursor()
        cursor.execute(sql, (
            customer.customer_name,
            customer.email,
            customer.phone_number,
            customer.address_line_1,
            customer.city
        ))
        connection.commit()
        new_customer = cursor.fetchone()
        return {
            "customer_id": new_customer[0],
            "customer_name": new_customer[1],
            "email": new_customer[2],
            "phone_number": new_customer[3],
            "address_line_1": new_customer[4],
            "city": new_customer[5],
        }
    except Exception as e:
        print(e)
        return {"error": str(e)}
    finally:
        cursor.close()

def update_single_customer(customer_id: int, customer: CustomerCreate):
    check_sql = "SELECT * FROM customerorders.customer WHERE customer_id = '{customer_id}'"
    exists = execute_query(check_sql)
    if not exists:
        raise HTTPException(status_code=404, detail="Customer not found")

    update_sql = """
        UPDATE customerorders.customer
        SET customer_name = '{customer.customer_name}', email = '{customer.email}', phone_number = '{customer.phone_number}', address_line_1 = '{customer.address_line_1}', city = '{customer.city}'
        WHERE customer_id = '{customer_id}'
        RETURNING customer_id, customer_name, email, phone_number, address_line_1, city
    """
    data = execute_query(update_sql)
    return data[0] if data else {"error": "Update failed"}

@app.get("/")
async def root():
    return {"message": "Hello World"}

@app.get('/customer')
async def get_customers(name: str = None):
    if name:
        return handle_get_customer(name)
    else:
        return get_all_customers()

@app.get("/product")
def all_products():
    # Query: SELECT product_id, product_name, selling_price FROM product#  Return as JSON list
    return get_all_products()

@app.get("/product/{product_id}")
def single_product(product_id: int):
    return get_single_products(product_id)

@app.get("/customer/{customer_id}")
def get_customer(customer_id: int):
    return get_single_customer(customer_id)

@app.post("/customer")
def create_customer(customer: CustomerCreate):
    return add_customer(customer)

@app.put("/customer/{customer_id}")
def update_customer(customer_id: int, customer: CustomerCreate):
    return update_single_customer(customer_id, customer)

