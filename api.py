from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from datetime import date
import psycopg2
from psycopg2.extras import RealDictCursor

app = FastAPI()

# Enable CORS so your HTML page can call this API
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, specify your domains
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Database configurations
DB_CONFIG_PROD = {
    "host": "redact",
    "database": "redact",
    "user": "redact",
    "password": "redact",
    "port": 5432
}

DB_CONFIG_ATHENA = {
    "host": "redact",
    "database": "redact",
    "user": "redact",
    "password": "redact",
    "port": 5432
}

# Channel type mapping
CHANNEL_MAPPING = {
    "0199947b-b0a0-7885-a32a-4cb744df96a5": "Website",
    "0199947b-b0a0-7885-a32a-5686afc4481e": "App",
    "0199947b-b0a0-7885-a32a-5f115333f817": "WhatsApp",
    "0199947b-b0a0-7885-a32a-67a4a63bf846": "Voice"
}

@app.get("/")
def read_root():
    return {"message": "Daily Metrics API is running"}

@app.get("/api/daily-metrics")
def get_daily_metrics(report_date: str = "2025-12-28", business_account_id: str = None):
    """Get daily metrics: revenue, transactions, new customers, items sold"""
    try:
        conn = psycopg2.connect(**DB_CONFIG_PROD)
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        base_query = """
            SELECT 
                COALESCE(SUM(total_order_value), 0) as total_revenue,
                COUNT(*) as total_transactions,
                COALESCE(SUM(number_of_items), 0) as items_sold
            FROM order_transactions 
            WHERE status = 'completed'
                AND DATE(created_at AT TIME ZONE 'EST') = %s
        """
        
        params = [report_date]
        
        if business_account_id:
            base_query += " AND business_account_id = %s"
            params.append(business_account_id)
        
        cursor.execute(base_query, params)
        metrics = cursor.fetchone()
        
        # Get new customers for the day
        customer_query = """
            SELECT COUNT(*) as new_customers
            FROM customers
            WHERE DATE(created_at AT TIME ZONE 'EST') = %s
        """
        
        customer_params = [report_date]
        
        if business_account_id:
            customer_query += " AND business_account_id = %s"
            customer_params.append(business_account_id)
        
        cursor.execute(customer_query, customer_params)
        new_customers = cursor.fetchone()
        
        cursor.close()
        conn.close()
        
        return {
            "total_revenue": float(metrics['total_revenue']),
            "total_transactions": metrics['total_transactions'],
            "items_sold": metrics['items_sold'],
            "new_customers": new_customers['new_customers'],
            "report_date": report_date
        }
    except Exception as e:
        return {
            "error": str(e),
            "total_revenue": 0,
            "total_transactions": 0,
            "items_sold": 0,
            "new_customers": 0
        }

@app.get("/api/daily-orders")
def get_daily_orders(report_date: str = "2025-12-28", business_account_id: str = None):
    """
    Get daily orders with customer details from both databases.
    Flow: order_transactions -> customers (get chatwoot_contact_id) -> contacts (get name & phone)
    """
    try:
        # Step 1: Connect to afto_prod_new and get order data with chatwoot_contact_id
        conn_prod = psycopg2.connect(**DB_CONFIG_PROD)
        cursor_prod = conn_prod.cursor(cursor_factory=RealDictCursor)
        
        query = """
            SELECT 
                ot.order_number,
                ot.id as order_id,
                ot.customer_id,
                c.chatwoot_contact_id,
                ot.total_order_value,
                ot.number_of_items,
                ot.status,
                ot.payment_status,
                ot.delivery_type,
                ot.created_at,
                ot.channel_type_id,
                ot.order_tax,
                ot.order_value_sub_total
            FROM order_transactions ot
            LEFT JOIN customers c ON ot.customer_id = c.id
            WHERE ot.status = 'completed'
                AND DATE(ot.created_at AT TIME ZONE 'EST') = %s
        """
        
        params = [report_date]
        
        if business_account_id:
            query += " AND ot.business_account_id = %s"
            params.append(business_account_id)
        
        query += " ORDER BY ot.created_at DESC LIMIT 100"
        
        cursor_prod.execute(query, params)
        orders = cursor_prod.fetchall()
        
        cursor_prod.close()
        conn_prod.close()
        
        # Step 2: If we have orders, get customer details from afto_athena_prod
        if orders:
            # Collect all chatwoot_contact_ids
            chatwoot_ids = [order['chatwoot_contact_id'] for order in orders if order['chatwoot_contact_id']]
            
            customer_details = {}
            
            if chatwoot_ids:
                conn_athena = psycopg2.connect(**DB_CONFIG_ATHENA)
                cursor_athena = conn_athena.cursor(cursor_factory=RealDictCursor)
                
                # Fetch customer details from contacts table
                cursor_athena.execute("""
                    SELECT 
                        id,
                        name,
                        phone_number,
                        email
                    FROM contacts
                    WHERE id = ANY(%s)
                """, (chatwoot_ids,))
                
                contacts = cursor_athena.fetchall()
                
                # Create a mapping of chatwoot_contact_id -> customer details
                for contact in contacts:
                    customer_details[contact['id']] = {
                        'name': contact['name'] or 'Guest',
                        'phone_number': contact['phone_number'] or 'N/A',
                        'email': contact['email']
                    }
                
                cursor_athena.close()
                conn_athena.close()
            
            # Step 3: Merge customer details into orders
            for order in orders:
                chatwoot_id = order['chatwoot_contact_id']
                
                if chatwoot_id and chatwoot_id in customer_details:
                    order['customer_name'] = customer_details[chatwoot_id]['name']
                    order['customer_phone'] = customer_details[chatwoot_id]['phone_number']
                    order['customer_email'] = customer_details[chatwoot_id]['email']
                else:
                    order['customer_name'] = 'Guest'
                    order['customer_phone'] = 'N/A'
                    order['customer_email'] = None
                
                # Map channel type
                channel_id = str(order['channel_type_id'])
                order['channel_name'] = CHANNEL_MAPPING.get(channel_id, 'Unknown')
                
                # Format timestamp
                if order['created_at']:
                    order['created_at'] = order['created_at'].isoformat()
                
                # Format phone for display (mask if needed)
                if order['customer_phone'] and order['customer_phone'] != 'N/A':
                    phone = order['customer_phone']
                    if len(phone) > 6:
                        order['customer_phone_display'] = phone[:6] + '...'
                    else:
                        order['customer_phone_display'] = phone
                else:
                    order['customer_phone_display'] = 'N/A'
        
        return {
            "orders": orders,
            "total_orders": len(orders),
            "report_date": report_date
        }
        
    except Exception as e:
        import traceback
        return {
            "error": str(e),
            "traceback": traceback.format_exc(),
            "orders": [],
            "total_orders": 0
        }

@app.get("/api/health")
def health_check():
    """Health check endpoint to verify database connectivity"""
    try:
        # Check afto_prod_new DB
        conn_prod = psycopg2.connect(**DB_CONFIG_PROD)
        cursor_prod = conn_prod.cursor()
        cursor_prod.execute("SELECT 1")
        cursor_prod.close()
        conn_prod.close()
        
        # Check afto_athena_prod DB
        conn_athena = psycopg2.connect(**DB_CONFIG_ATHENA)
        cursor_athena = conn_athena.cursor()
        cursor_athena.execute("SELECT 1")
        cursor_athena.close()
        conn_athena.close()
        
        return {
            "status": "healthy",
            "afto_prod_new": "connected",
            "afto_athena_prod": "connected"
        }
    except Exception as e:
        return {
            "status": "unhealthy",
            "error": str(e)
        }

@app.get("/api/test-channel-mapping")
def test_channel_mapping():
    """Test endpoint to verify channel type mapping"""
    try:
        conn = psycopg2.connect(**DB_CONFIG_PROD)
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        cursor.execute("""
            SELECT DISTINCT channel_type_id, COUNT(*) as count
            FROM order_transactions
            GROUP BY channel_type_id
        """)
        
        channels = cursor.fetchall()
        
        result = []
        for channel in channels:
            channel_id = str(channel['channel_type_id'])
            result.append({
                "channel_type_id": channel_id,
                "mapped_name": CHANNEL_MAPPING.get(channel_id, "Unknown"),
                "order_count": channel['count']
            })
        
        cursor.close()
        conn.close()
        
        return {
            "channels": result,
            "mapping": CHANNEL_MAPPING
        }
        
    except Exception as e:
        return {"error": str(e)}
