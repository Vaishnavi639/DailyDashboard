from dagster import op, Output, OpExecutionContext
from datetime import datetime, timedelta
from services.daily_metrics_service import DailyMetricsService
from services.email_service import EmailService
from services.email_template_generator import EmailTemplateGenerator

@op
def get_business_accounts_op(context: OpExecutionContext):
    """Get all active business accounts"""
    context.log.info("Fetching active business accounts...")
    
    metrics_service = DailyMetricsService()
    accounts = metrics_service.get_business_accounts()
    
    context.log.info(f"Found {len(accounts)} active business accounts")
    
    return accounts

@op
def generate_daily_report_op(context: OpExecutionContext, business_account: dict):
    """Generate daily report for a single business account"""
    business_id = str(business_account['id'])
    business_name = business_account['business_name']
    business_email = business_account['business_email']
    
    # Get yesterday's date (report runs at 10 PM for previous day)
    report_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    
    context.log.info(f"Generating report for {business_name} ({business_id}) - Date: {report_date}")
    
    metrics_service = DailyMetricsService()
    
    # Get metrics
    metrics = metrics_service.get_daily_metrics(business_id, report_date)
    
    # Get orders
    orders = metrics_service.get_daily_orders(business_id, report_date)
    
    # Get flyer data
    flyer_data = metrics_service.get_weekly_flyer_performance(business_id)
    
    # Generate HTML
    template_generator = EmailTemplateGenerator()
    html_content = template_generator.generate_daily_report_html(
        business_name=business_name,
        metrics=metrics,
        orders=orders,
        flyer_data=flyer_data,
        report_date=report_date
    )
    
    context.log.info(f"Report generated for {business_name}: Revenue=${metrics['total_revenue']:.2f}, Orders={metrics['total_transactions']}")
    
    return {
        "business_id": business_id,
        "business_name": business_name,
        "business_email": business_email,
        "html_content": html_content,
        "report_date": report_date,
        "metrics": metrics
    }

@op
def send_email_op(context: OpExecutionContext, report_data: dict):
    """Send email report"""
    business_email = report_data['business_email']
    business_name = report_data['business_name']
    
    context.log.info(f"Sending email to {business_email}...")
    
    email_service = EmailService()
    result = email_service.send_daily_report(
        to_email=business_email,
        business_name=business_name,
        html_content=report_data['html_content'],
        report_date=report_data['report_date']
    )
    
    if result['success']:
        context.log.info(f"✓ Email sent successfully to {business_email}")
    else:
        context.log.error(f"✗ Failed to send email to {business_email}: {result['error']}")
    
    return result
