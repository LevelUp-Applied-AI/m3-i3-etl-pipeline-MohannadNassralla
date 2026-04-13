import pandas as pd
from sqlalchemy import create_engine
import os

def extract(engine):
    print("--- Stage 1: Extracting Data ---")
    tables = ["customers", "products", "orders", "order_items"]
    data_dict = {}
    
    for table in tables:
        data_dict[table] = pd.read_sql(f"SELECT * FROM {table}", engine)
        print(f"Extracted {len(data_dict[table])} rows from {table}")
        
    return data_dict

def transform(data_dict):
    print("\n--- Stage 2: Transforming Data ---")
    
    customers = data_dict["customers"]
    products = data_dict["products"]
    orders = data_dict["orders"]
    order_items = data_dict["order_items"]

    # 1. Join orders with order_items and products
    df = order_items.merge(orders, on="order_id")
    df = df.merge(products, on="product_id")
    df = df.merge(customers, on="customer_id")

    # 2. Compute line_total
    df['line_total'] = df['quantity'] * df['unit_price']

    # 3. Filter out cancelled orders and suspicious quantities (> 100)
    initial_count = len(df)
    df = df[df['status'] != 'cancelled']
    df = df[df['quantity'] <= 100]
    print(f"Filtered out {initial_count - len(df)} invalid/cancelled rows")

    # 4. Aggregate to customer-level summary
    
    customer_summary = df.groupby(['customer_id', 'customer_name']).agg(
        total_orders=('order_id', 'nunique'),
        total_revenue=('line_total', 'sum')
    ).reset_index()

    customer_summary['avg_order_value'] = customer_summary['total_revenue'] / customer_summary['total_orders']

    cat_revenue = df.groupby(['customer_id', 'category'])['line_total'].sum().reset_index()
    top_cat = cat_revenue.sort_values(['customer_id', 'line_total'], ascending=[True, False]) \
                         .drop_duplicates('customer_id')
    
    customer_summary = customer_summary.merge(top_cat[['customer_id', 'category']], on='customer_id')
    customer_summary.rename(columns={'name': 'customer_name', 'category': 'top_category'}, inplace=True)

    return customer_summary

def validate(df):
    """التحقق من جودة البيانات بعد التحويل"""
    print("\n--- Stage 3: Validating Data ---")
    checks = {
        "No Null IDs/Names": df[['customer_id', 'customer_name']].notnull().all().all(),
        "Total Revenue > 0": (df['total_revenue'] > 0).all(),
        "No Duplicate IDs": df['customer_id'].is_unique,
        "Total Orders > 0": (df['total_orders'] > 0).all()
    }

    for check, passed in checks.items():
        status = "PASS" if passed else "FAIL"
        print(f"{check}: {status}")
        if not passed:
            raise ValueError(f"Critical Data Quality Check Failed: {check}")

    return checks

def load(df, engine, csv_path):
    """تحميل البيانات إلى قاعدة البيانات وحفظ نسخة CSV"""
    print("\n--- Stage 4: Loading Data ---")
    
    # إنشاء المجلد إذا لم يكن موجوداً
    os.makedirs(os.path.dirname(csv_path), exist_ok=True)
    
    # حفظ في PostgreSQL
    df.to_sql("customer_analytics", engine, if_exists="replace", index=False)
    
    # حفظ في CSV
    df.to_csv(csv_path, index=False)
    
    print(f"Successfully loaded {len(df)} rows to 'customer_analytics' table and {csv_path}")

def main():
    # إعدادات الاتصال (تأكد من مطابقة بيانات الدوكر لديك)
    # ملاحظة: استخدم localhost لأنك تشغل الكود من جهازك والدوكر فاتح بورت 5432
    conn_str = "postgresql://postgres:postgres@localhost:5432/amman_market"
    engine = create_engine(conn_str)
    output_path = "output/customer_analytics.csv"

    try:
        data = extract(engine)
        summary_df = transform(data)
        validate(summary_df)
        load(summary_df, engine, output_path)
        print("\nETL Pipeline Completed Successfully! ✅")
    except Exception as e:
        print(f"\nETL Pipeline Failed ❌: {e}")

if __name__ == "__main__":
    main()