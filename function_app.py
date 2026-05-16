import json
from jsonschema import validate, ValidationError
from typing import Any

import azure.functions as func
import logging

logging.basicConfig(level=logging.INFO)
app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

def validate_body(body: dict[str, Any]) -> None:
    """Validates schema of the request body."""
    schema = {
        "type": "object",
        "properties": {
            "order_id": {"type": "string"},
            "customer": {"type": "string"},
            "items": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "product": {"type": "string"},
                        "price": {"type": "number"},
                        "quantity": {"type": "integer"}
                    },
                    "required": ["product", "price", "quantity"]
                }
            }
        },
        "required": ["order_id", "customer", "items"]
    }
    validate(instance=body, schema=schema)

def generate_data(order: dict[str, Any], total_amount: float, status: str) -> str:
    return json.dumps({
        'PartitionKey': status,
        'RowKey': order.get('order_id'),
        'order_id': order.get('order_id'),
        'customer': order.get('customer'),
        'status': status,
        'total': total_amount
    }) 

def send_email(to_email: str, subject: str, body: str) -> None:
    # Here you would implement the logic to send an email using smtplib or any email service provider
    logging.info(f"Sending email to {to_email} with subject '{subject}' and body '{body}'")
    pass

@app.route(route="receiving_products")
@app.queue_output(arg_name="msg", 
                  queue_name="pending-orders", 
                  connection="AzureWebJobsStorage")
def receiving_products(req: func.HttpRequest, msg: func.Out[str]) -> func.HttpResponse:
    try:
        body: dict[str, str | int] = req.get_json()
        logging.info(f"Received body request: {body}")
        if not body:
            return func.HttpResponse(json.dumps({"error": "Request body is empty"}), status_code=400)
        validate_body(body)
        msg.set(json.dumps(body))
        return func.HttpResponse(json.dumps({"message": "Request processed successfully"}), status_code=202)
    except ValueError as e:
        logging.error(f"Invalid JSON format: {e}")
        return func.HttpResponse(json.dumps({"error": "Invalid JSON format"}), status_code=400)
    except ValidationError as e:
        logging.error(f"Validation error: {e}")
        return func.HttpResponse(json.dumps({"error": str(e),}), status_code=400) 
    

@app.queue_trigger(arg_name="azqueue", queue_name="pending-orders",
                               connection="AzureWebJobsStorage") 
@app.queue_output(arg_name="analyse_orders_queue", 
                  queue_name="analyse-orders", 
                  connection="AzureWebJobsStorage")
@app.queue_output(arg_name="refused_orders_queue", 
                  queue_name="refused-orders", 
                  connection="AzureWebJobsStorage")
@app.table_output(arg_name="approved_orders_table",
                  table_name="approvedorders",
                  connection="AzureWebJobsStorage")
def verify_purchase_order(azqueue: func.QueueMessage, analyse_orders_queue: func.Out[str], refused_orders_queue: func.Out[str], approved_orders_table: func.Out[str]) -> None:
    logging.info(f'Python Queue trigger processed a message: {azqueue=}')
    try:
        if not azqueue:
            logging.error("Queue message is empty")
            raise ValueError("Queue message is empty")
        order: dict[str, Any] = azqueue.get_json()
        products: list[dict[str, Any]] = order.get("items", [])
        total_amount: float = round(sum(float(item.get("price", 0)) * int(item.get("quantity", 0)) for item in products), 2)  
        logging.info(f"Calculated total amount for order {type(order)}: {order}")
        if total_amount > 5000:
            logging.warning(f"Order {order.get('order_id')} has a high total amount: {total_amount}")
            data = generate_data(order, total_amount, "pending")
            analyse_orders_queue.set(data)
        elif total_amount <= 0:
            logging.warning(f"Order {order.get('order_id')} has a non-positive total amount: {total_amount}")
            data = generate_data(order, total_amount, "refused")
            refused_orders_queue.set(data)
        else:
            logging.info(f"Order {order.get('order_id')} has a normal total amount: {total_amount}")
            data = generate_data(order, total_amount, "approved")
            approved_orders_table.set(data)
    except ValueError as e:
        logging.error(f"Invalid JSON format: {e}")
        raise
    except Exception as e:
        logging.error(f"Missing key in order data: {e}")
        raise

@app.queue_trigger(arg_name="azqueue", queue_name="analyse-orders",
                               connection="AzureWebJobsStorage") 
@app.table_output(arg_name="approved_orders_table",
                  table_name="analyseordersemailsending",
                  connection="AzureWebJobsStorage")
def send_email_analyse_order(azqueue: func.QueueMessage, approved_orders_table: func.Out[str]) -> None:
    logging.info('Python Queue trigger processed a message')
    try:
        if not azqueue:
            logging.error("Queue message is empty")
            raise ValueError("Queue message is empty")
        order: dict[str, str | float] = azqueue.get_json()
        logging.info(f"Processing order for analysis: {type(order)}, {order}")
        send_email(to_email="analyst@example.com", subject="Order Analysis Required", body=f"Please analyze the following order: {order}")
        data = generate_data(order, float(order.get("total", 0)), "send_email")
        approved_orders_table.set(data)
    except ValueError as e:
        logging.error(f"Invalid JSON format: {e}")
        raise
    except Exception as e:
        logging.error(f"Missing key in order data: {e}")
        raise
