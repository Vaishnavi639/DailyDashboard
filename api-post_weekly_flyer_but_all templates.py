from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from datetime import date, datetime
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

@app.get("/api/weekly-flyer-performance")
def get_weekly_flyer_performance(business_account_id: str = None):
    """
    Get weekly flyer products performance showing daily sales breakdown.
    Returns data in format: {product_name: {day1: quantity, day2: quantity, ...}, totals: {...}}
    """
    try:
        conn = psycopg2.connect(**DB_CONFIG_PROD)
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        # Step 1: Get active Weekly Flyer template
        # First try without business_account_id filter to see if template exists
        template_query = """
            SELECT id, name, start_date, end_date, status, business_account_id
            FROM product_templates
            WHERE (name = 'Weekly Flyer' OR name ILIKE '%weekly%flyer%')
                AND status = 'active'
            ORDER BY created_at DESC
            LIMIT 1
        """
        
        cursor.execute(template_query)
        template = cursor.fetchone()
        
        # If business_account_id is provided and template doesn't match, try to find one that does
        if business_account_id and template and str(template.get('business_account_id')) != business_account_id:
            cursor.execute("""
                SELECT id, name, start_date, end_date, status, business_account_id
                FROM product_templates
                WHERE (name = 'Weekly Flyer' OR name ILIKE '%weekly%flyer%')
                    AND status = 'active'
                    AND business_account_id = %s
                ORDER BY created_at DESC
                LIMIT 1
            """, (business_account_id,))
            template_filtered = cursor.fetchone()
            if template_filtered:
                template = template_filtered
        
        if not template:
            # Check if ANY templates exist
            cursor.execute("""
                SELECT 
                    COUNT(*) as count, 
                    COUNT(*) FILTER (WHERE status = 'active') as active_count,
                    COUNT(*) FILTER (WHERE name ILIKE '%weekly%flyer%') as weekly_flyer_count,
                    COUNT(*) FILTER (WHERE name ILIKE '%weekly%flyer%' AND status = 'active') as active_weekly_flyer_count
                FROM product_templates
            """)
            counts = cursor.fetchone()
            
            cursor.close()
            conn.close()
            return {
                "error": f"No active Weekly Flyer template found",
                "products": [],
                "template_info": None,
                "debug_info": {
                    "total_templates": counts['count'],
                    "active_templates": counts['active_count'],
                    "weekly_flyer_templates": counts['weekly_flyer_count'],
                    "active_weekly_flyer_templates": counts['active_weekly_flyer_count'],
                    "business_account_id_filter": business_account_id,
                    "search_criteria": "name = 'Weekly Flyer' OR name ILIKE '%weekly%flyer%' AND status = 'active'"
                }
            }
        
        template_id = template['id']
        start_date = template['start_date']
        end_date = template['end_date']
        
        # Step 2: Get all sections for this template
        cursor.execute("""
            SELECT id, title, serial_number
            FROM product_template_sections
            WHERE template_id = %s
            ORDER BY serial_number
        """, (template_id,))
        
        sections = cursor.fetchall()
        section_ids = [str(section['id']) for section in sections]
        
        if not section_ids:
            cursor.close()
            conn.close()
            return {
                "error": "No sections found for Weekly Flyer template",
                "products": [],
                "template_info": template
            }
        
        # Step 3: Get all products in these sections (cast to UUID array)
        cursor.execute("""
            SELECT DISTINCT pti.product_retailer_id, p.name
            FROM product_template_items pti
            JOIN products p ON pti.product_retailer_id = p.retailer_id
            WHERE pti.section_id = ANY(%s::uuid[])
            ORDER BY p.name
        """, (section_ids,))
        
        template_products = cursor.fetchall()
        product_retailer_ids = [str(p['product_retailer_id']) for p in template_products]
        
        if not product_retailer_ids:
            cursor.close()
            conn.close()
            return {
                "error": "No products found in Weekly Flyer sections",
                "products": [],
                "template_info": template
            }
        
        # Step 4: Get daily sales data for these products within template date range (cast to UUID array)
        cursor.execute("""
            SELECT 
                oi.product_retailer_id,
                p.name as product_name,
                DATE(ot.created_at AT TIME ZONE 'EST') as sale_date,
                SUM(oi.quantity) as total_quantity,
                SUM(oi.quantity * oi.unit_price) as total_revenue
            FROM order_items oi
            JOIN order_transactions ot ON oi.order_id = ot.id
            JOIN products p ON oi.product_retailer_id = p.retailer_id
            WHERE oi.product_retailer_id = ANY(%s::uuid[])
                AND ot.status = 'completed'
                AND DATE(ot.created_at AT TIME ZONE 'EST') >= DATE(%s)
                AND DATE(ot.created_at AT TIME ZONE 'EST') <= DATE(%s)
            GROUP BY oi.product_retailer_id, p.name, DATE(ot.created_at AT TIME ZONE 'EST')
            ORDER BY p.name, sale_date
        """, (product_retailer_ids, start_date, end_date))
        
        sales_data = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        # Step 5: Format data for frontend
        # Calculate number of days in the flyer period
        start_dt = start_date if isinstance(start_date, datetime) else datetime.fromisoformat(str(start_date))
        end_dt = end_date if isinstance(end_date, datetime) else datetime.fromisoformat(str(end_date))
        num_days = (end_dt.date() - start_dt.date()).days + 1
        
        # Initialize product data structure
        products_performance = {}
        for product in template_products:
            product_name = product['name']
            products_performance[product_name] = {
                'product_retailer_id': str(product['product_retailer_id']),
                'daily_sales': {f'day_{i+1}': 0 for i in range(num_days)},
                'total_quantity': 0,
                'total_revenue': 0.0
            }
        
        # Fill in actual sales data
        for sale in sales_data:
            product_name = sale['product_name']
            sale_date = sale['sale_date']
            
            # Calculate which day of the flyer this sale occurred on
            sale_dt = sale_date if isinstance(sale_date, datetime) else datetime.fromisoformat(str(sale_date))
            day_offset = (sale_dt.date() - start_dt.date()).days
            
            if 0 <= day_offset < num_days:
                day_key = f'day_{day_offset + 1}'
                if product_name in products_performance:
                    products_performance[product_name]['daily_sales'][day_key] += sale['total_quantity']
                    products_performance[product_name]['total_quantity'] += sale['total_quantity']
                    products_performance[product_name]['total_revenue'] += float(sale['total_revenue'])
        
        # Convert to list format for frontend
        products_list = []
        for product_name, data in products_performance.items():
            product_entry = {
                'product_name': product_name,
                'total_quantity': data['total_quantity'],
                'total_revenue': data['total_revenue']
            }
            # Add daily sales
            for day_key, quantity in data['daily_sales'].items():
                product_entry[day_key] = quantity
            
            products_list.append(product_entry)
        
        # Sort by total quantity sold (descending)
        products_list.sort(key=lambda x: x['total_quantity'], reverse=True)
        
        return {
            "products": products_list,
            "template_info": {
                "id": str(template['id']),
                "name": template['name'],
                "start_date": start_date.isoformat() if hasattr(start_date, 'isoformat') else str(start_date),
                "end_date": end_date.isoformat() if hasattr(end_date, 'isoformat') else str(end_date),
                "num_days": num_days,
                "status": template['status']
            },
            "total_products": len(products_list)
        }
        
    except Exception as e:
        import traceback
        return {
            "error": str(e),
            "traceback": traceback.format_exc(),
            "products": [],
            "template_info": None
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

@app.get("/api/debug-templates")
def debug_templates(business_account_id: str = None):
    """Debug endpoint to see all templates in the database"""
    try:
        conn = psycopg2.connect(**DB_CONFIG_PROD)
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        query = """
            SELECT id, name, status, start_date, end_date, created_at, business_account_id
            FROM product_templates
            ORDER BY created_at DESC
        """
        
        params = []
        if business_account_id:
            query = """
                SELECT id, name, status, start_date, end_date, created_at, business_account_id
                FROM product_templates
                WHERE business_account_id = %s
                ORDER BY created_at DESC
            """
            params = [business_account_id]
        
        cursor.execute(query, params)
        templates = cursor.fetchall()
        
        # Convert dates to strings for JSON serialization
        for template in templates:
            if template.get('start_date'):
                template['start_date'] = template['start_date'].isoformat()
            if template.get('end_date'):
                template['end_date'] = template['end_date'].isoformat()
            if template.get('created_at'):
                template['created_at'] = template['created_at'].isoformat()
            if template.get('id'):
                template['id'] = str(template['id'])
            if template.get('business_account_id'):
                template['business_account_id'] = str(template['business_account_id'])
        
        # Find weekly flyer templates
        weekly_flyers = [t for t in templates if t.get('name') and ('weekly flyer' in t['name'].lower())]
        active_weekly_flyers = [t for t in weekly_flyers if t.get('status') == 'active']
        
        cursor.close()
        conn.close()
        
        return {
            "templates": templates,
            "total": len(templates),
            "weekly_flyer_templates": weekly_flyers,
            "active_weekly_flyer_count": len(active_weekly_flyers),
            "business_account_id_filter": business_account_id
        }
        
    except Exception as e:
        import traceback
        return {
            "error": str(e),
            "traceback": traceback.format_exc()
        }
