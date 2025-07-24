from fastapi import FastAPI
from pydantic import BaseModel
import pandas as pd
import psycopg2

app = FastAPI()

connection = psycopg2.connect(database="postgres", user="postgres.qollpkburpqlzxkvliyu", password="Chowmein.08011968", host="aws-0-eu-west-2.pooler.supabase.com", port=5432)

def execute_query(sql: str, params=None, fetch=True):
    try:
        cursor = connection.cursor()
        print(sql)
        if params:
            cursor.execute(sql, params)
        else:
            cursor.execute(sql)
        if fetch:
            record = cursor.fetchall()
            return record
        else:
            connection.commit()
            return None
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
    params = (
        customer.customer_name,
        customer.email,
        customer.phone_number,
        customer.address_line_1,
        customer.city
    )
    data = execute_query(sql, params)
    return data[0]

def update_single_customer(customer_id: int, customer: CustomerCreate):
    check_sql = "SELECT * FROM customerorders.customer WHERE customer_id = %s"
    exists = execute_query(check_sql, (customer_id,))
    if not exists:
        return {"error": "Customer not found"}

    update_sql = """
        UPDATE customerorders.customer
        SET customer_name = %s,
            email = %s,
            phone_number = %s,
            address_line_1 = %s,
            city = %s
        WHERE customer_id = %s
        RETURNING customer_id, customer_name, email, phone_number, address_line_1, city
    """
    params = (
        customer.customer_name,
        customer.email,
        customer.phone_number,
        customer.address_line_1,
        customer.city,
        customer_id
    )
    data = execute_query(update_sql, params)
    return data[0] if data else {"error": "Update failed"}

def get_order_details(order_id: int):
    sql = """
        SELECT o.order_id, o.order_date, o.total_amount, c.customer_name, c.email, s.status_name
        FROM customerorders.orders o
        JOIN customerorders.customer c ON o.customer_id = c.customer_id
        JOIN customerorders.order_status s ON o.order_status_id = s.order_status_id
        WHERE o.order_id = %s
    """
    data = execute_query(sql, (order_id,))

    if data and len(data) > 0:
        row = data[0]
        return {
            "order_id": row[0],
            "order_date": row[1],
            "total_amount": row[2],
            "customer_name": row[3],
            "customer_email": row[4],
            "status_name": row[5],
        }
    else:
        return {"error": "Order not found"}

def get_order_items(order_id: int):
    sql = """
        SELECT ol.quantity, p.product_name, p.selling_price, (ol.quantity * p.selling_price) as line_total
        FROM customerorders.order_line ol
        JOIN customerorders.product p ON ol.product_id = p.product_id
        WHERE ol.order_id = %s
    """
    rows = execute_query(sql, (order_id,))
    items = []
    if rows:
        for row in rows:
            items.append({
                "quantity": row[0],
                "product_name": row[1],
                "selling_price": row[2],
                "line_total": row[3],
            })
    return items

def get_customer_orders(customer_id: int):
    sql = """
        SELECT o.order_id, o.order_date, o.total_amount, s.status_name
        FROM customerorders.orders o
        JOIN customerorders.order_status s ON o.order_status_id = s.order_status_id
        WHERE o.customer_id = %s
        ORDER BY o.order_date DESC
    """
    rows = execute_query(sql, (customer_id,))
    orders = []
    if rows:
        for row in rows:
            orders.append({
                "order_id": row[0],
                "order_date": row[1],
                "total_amount": row[2],
                "status_name": row[3],
            })
    return orders

def update_status(order_id: int, new_status_id: int):
    sql = """ 
        UPDATE customerorders.orders 
        SET order_status_id = %s
        WHERE order_id = %s
        RETURNING order_id, order_status_id
      """
    updated_order = execute_query(sql, (new_status_id, order_id))

    if not updated_order:
        return {"error": "Order not found or status not updated"}

    return {
        "order_id": updated_order[0][0],
        "new_status_id": updated_order[0][1],
    }

def delete_order_by_id(order_id: int):
    delete_ol_sql = """
        DELETE FROM customerorders.order_line
        WHERE order_id = %s
    """
    execute_query(delete_ol_sql, (order_id,), fetch=False)
    
    delete_o_sql = """
        DELETE FROM customerorders.orders
        WHERE order_id = %s
        RETURNING order_id
    """
    delete_order = execute_query(delete_o_sql, (order_id,))
    
    if not delete_order:
        return {"error": "Order not deleted"}
    
    return{
         "message": f"Order {delete_order[0][0]} and its items deleted successfully"
    }
    

@app.get("/")
async def root():
    return {"message": "Hello World"}

@app.get('/customer')
async def get_customers():
        return get_all_customers()
    
@app.get("/customers/{name}")
def get_customer_by_name(name: str):
    return handle_get_customer(name)

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

@app.get("/orders/{order_id}")
def order_details(order_id: int):
    return get_order_details(order_id)

@app.get("/orders/{order_id}/items")
def order_items(order_id: int):
    return get_order_items(order_id)

@app.get("/customers/{customer_id}/orders")
def customer_orders(customer_id: int):
    return get_customer_orders(customer_id)

@app.put("/orders/{order_id}/status")
def update_order_status(order_id: int, new_status_id: int):
    return update_status(order_id, new_status_id)

@app.delete("/orders/{order_id}")
def delete_order(order_id: int):
    return delete_order_by_id(order_id)