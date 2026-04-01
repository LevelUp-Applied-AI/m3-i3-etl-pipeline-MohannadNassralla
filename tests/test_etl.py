import pytest
import pandas as pd
from etl_pipeline import transform, validate

def test_transform_filters_cancelled():
    
    data = {
        "customers": pd.DataFrame({'customer_id': [1], 'customer_name': ['Ali']}),
        "products": pd.DataFrame({'product_id': [101], 'category': ['Tech'], 'unit_price': [100]}),
        "orders": pd.DataFrame({'order_id': [1, 2], 'customer_id': [1, 1], 'status': ['completed', 'cancelled']}),
        "order_items": pd.DataFrame({'order_id': [1, 2], 'product_id': [101, 101], 'quantity': [1, 1]})
    }
    result = transform(data)
    assert result.iloc[0]['total_orders'] == 1

def test_transform_filters_suspicious_quantity():
    data = {
        "customers": pd.DataFrame({'customer_id': [1], 'customer_name': ['Ali']}),
        "products": pd.DataFrame({'product_id': [101], 'category': ['Tech'], 'unit_price': [10]}),
        "orders": pd.DataFrame({'order_id': [1], 'customer_id': [1], 'status': ['completed']}),
        "order_items": pd.DataFrame({'order_id': [1], 'product_id': [101], 'quantity': [101]}) # كمية مشبوهة
    }
    result = transform(data)
    
    assert len(result) == 0

def test_validate_catches_nulls():
    df_with_null = pd.DataFrame({
        'customer_id': [None], 
        'customer_name': ['Unknown'],
        'total_orders': [1],
        'total_revenue': [100]
    })
    with pytest.raises(ValueError, match="No Null IDs/Names"):
        validate(df_with_null)