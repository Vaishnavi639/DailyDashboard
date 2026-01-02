import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime, timedelta
from config.settings import DB_CONFIG_PROD, DB_CONFIG_ATHENA, CHANNEL_MAPPING

class DailyMetricsService:
    
    def get_business_accounts(self):
        """
        Get all active business accounts with email addresses.
        Uses direct database query for performance.
        """
        conn = psycopg2.connect(**DB_CONFIG_PROD)
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        # Query matches actual schema: 'name' and 'email' columns
        # Using aliases so rest of code doesn't need changes
        cursor.execute("""
            SELECT 
                id, 
                name as business_name, 
                email as business_email
            FROM business_accounts
            WHERE email IS NOT NULL
            ORDER BY name
        """)
        
        accounts = cursor.fetchall()
        cursor.close()
        conn.close()
        
        return accounts
    
    def get_daily_metrics(self, business_account_id: str, report_date: str):
        """Get daily metrics for a business"""
        conn = psycopg2.connect(**DB_CONFIG_PROD)
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        # Get order metrics
        cursor.execute("""
            SELECT 
                COALESCE(SUM(total_order_value), 0) as total_revenue,
                COUNT(*) as total_transactions,
                COALESCE(SUM(number_of_items), 0) as items_sold
            FROM order_transactions 
            WHERE status = 'completed'
                AND DATE(created_at AT TIME ZONE 'EST') = %s
                AND business_account_id = %s
        """, (report_date, business_account_id))
        
        metrics = cursor.fetchone()
        
        # Get new customers
        cursor.execute("""
            SELECT COUNT(*) as new_customers
            FROM customers
            WHERE DATE(created_at AT TIME ZONE 'EST') = %s
                AND business_account_id = %s
        """, (report_date, business_account_id))
        
        new_customers = cursor.fetchone()
        
        cursor.close()
        conn.close()
        
        return {
            "total_revenue": float(metrics['total_revenue']),
            "total_transactions": metrics['total_transactions'],
            "items_sold": metrics['items_sold'],
            "new_customers": new_customers['new_customers']
        }
    
    def get_daily_orders(self, business_account_id: str, report_date: str, limit: int = 50):
        """Get daily orders with customer details"""
        conn_prod = psycopg2.connect(**DB_CONFIG_PROD)
        cursor_prod = conn_prod.cursor(cursor_factory=RealDictCursor)
        
        cursor_prod.execute("""
            SELECT 
                ot.order_number,
                ot.customer_id,
                c.chatwoot_contact_id,
                ot.total_order_value,
                ot.number_of_items,
                ot.status,
                ot.delivery_type,
                ot.created_at,
                ot.channel_type_id
            FROM order_transactions ot
            LEFT JOIN customers c ON ot.customer_id = c.id
            WHERE ot.status = 'completed'
                AND DATE(ot.created_at AT TIME ZONE 'EST') = %s
                AND ot.business_account_id = %s
            ORDER BY ot.created_at DESC
            LIMIT %s
        """, (report_date, business_account_id, limit))
        
        orders = cursor_prod.fetchall()
        cursor_prod.close()
        conn_prod.close()
        
        if not orders:
            return []
        
        # Get customer details from Athena DB
        chatwoot_ids = [order['chatwoot_contact_id'] for order in orders if order['chatwoot_contact_id']]
        customer_details = {}
        
        if chatwoot_ids:
            conn_athena = psycopg2.connect(**DB_CONFIG_ATHENA)
            cursor_athena = conn_athena.cursor(cursor_factory=RealDictCursor)
            
            cursor_athena.execute("""
                SELECT id, name, phone_number
                FROM contacts
                WHERE id = ANY(%s)
            """, (chatwoot_ids,))
            
            contacts = cursor_athena.fetchall()
            
            for contact in contacts:
                customer_details[contact['id']] = {
                    'name': contact['name'] or 'Guest',
                    'phone_number': contact['phone_number'] or 'N/A'
                }
            
            cursor_athena.close()
            conn_athena.close()
        
        # Merge customer details
        for order in orders:
            chatwoot_id = order['chatwoot_contact_id']
            
            if chatwoot_id and chatwoot_id in customer_details:
                order['customer_name'] = customer_details[chatwoot_id]['name']
                order['customer_phone'] = customer_details[chatwoot_id]['phone_number']
            else:
                order['customer_name'] = 'Guest'
                order['customer_phone'] = 'N/A'
            
            order['channel_name'] = CHANNEL_MAPPING.get(str(order['channel_type_id']), 'Unknown')
        
        return orders
    
    def get_weekly_flyer_performance(self, business_account_id: str):
        """Get weekly flyer products performance with daily breakdown"""
        try:
            conn = psycopg2.connect(**DB_CONFIG_PROD)
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            
            # Get active Weekly Flyer template
            cursor.execute("""
                SELECT id, name, start_date, end_date
                FROM product_templates
                WHERE (name = 'Weekly Flyer' OR name ILIKE '%%weekly%%flyer%%')
                    AND status = 'active'
                    AND business_account_id = %s
                ORDER BY created_at DESC
                LIMIT 1
            """, (business_account_id,))
            
            template = cursor.fetchone()
            
            if not template:
                cursor.close()
                conn.close()
                return None
            
            # Get sections
            cursor.execute("""
                SELECT id FROM product_template_sections
                WHERE template_id = %s
            """, (template['id'],))
            
            sections = cursor.fetchall()
            section_ids = [str(s['id']) for s in sections]
            
            if not section_ids:
                cursor.close()
                conn.close()
                return None
            
            # Get all products in the flyer
            cursor.execute("""
                SELECT DISTINCT pti.product_retailer_id, p.name
                FROM product_template_items pti
                JOIN products p ON pti.product_retailer_id = p.retailer_id
                WHERE pti.section_id = ANY(%s::uuid[])
                ORDER BY p.name
            """, (section_ids,))
            
            products = cursor.fetchall()
            product_ids = [str(p['product_retailer_id']) for p in products]
            
            if not product_ids:
                cursor.close()
                conn.close()
                return None
            
            # Get sales data with actual dates
            cursor.execute("""
                SELECT 
                    oi.product_retailer_id,
                    p.name as product_name,
                    DATE(ot.created_at AT TIME ZONE 'EST') as sale_date,
                    SUM(oi.quantity) as quantity
                FROM order_items oi
                JOIN order_transactions ot ON oi.order_id = ot.id
                JOIN products p ON oi.product_retailer_id = p.retailer_id
                WHERE oi.product_retailer_id = ANY(%s::uuid[])
                    AND ot.status = 'completed'
                    AND DATE(ot.created_at AT TIME ZONE 'EST') >= DATE(%s)
                    AND DATE(ot.created_at AT TIME ZONE 'EST') <= DATE(%s)
                GROUP BY oi.product_retailer_id, p.name, DATE(ot.created_at AT TIME ZONE 'EST')
                ORDER BY p.name, sale_date
            """, (product_ids, template['start_date'], template['end_date']))
            
            sales = cursor.fetchall()
            cursor.close()
            conn.close()
            
            # Format data with daily breakdown
            start_dt = template['start_date'] if isinstance(template['start_date'], datetime) else datetime.fromisoformat(str(template['start_date']))
            end_dt = template['end_date'] if isinstance(template['end_date'], datetime) else datetime.fromisoformat(str(template['end_date']))
            
            # Generate all dates in the range
            all_dates = []
            current_date = start_dt.date() if hasattr(start_dt, 'date') else start_dt
            end_date = end_dt.date() if hasattr(end_dt, 'date') else end_dt
            
            while current_date <= end_date:
                all_dates.append(current_date)
                current_date += timedelta(days=1)
            
            # Build product sales structure with all dates
            product_sales_map = {}
            for product in products:
                product_name = product['name']
                product_sales_map[product_name] = {
                    'product_retailer_id': str(product['product_retailer_id']),
                    'daily_sales': {date: 0 for date in all_dates},
                    'total_quantity': 0
                }
            
            # Fill in actual sales data
            for sale in sales:
                product_name = sale['product_name']
                sale_date = sale['sale_date']
                quantity = sale['quantity']
                
                if isinstance(sale_date, datetime):
                    sale_date = sale_date.date()
                
                if product_name in product_sales_map and sale_date in all_dates:
                    product_sales_map[product_name]['daily_sales'][sale_date] = quantity
                    product_sales_map[product_name]['total_quantity'] += quantity
            
            return {
                'template': template,
                'products': products,
                'sales': sales,
                'all_dates': all_dates,
                'product_sales_map': product_sales_map
            }
        except Exception as e:
            print(f"Error in get_weekly_flyer_performance: {e}")
            import traceback
            traceback.print_exc()
            if 'cursor' in locals():
                cursor.close()
            if 'conn' in locals():
                conn.close()
            return None
