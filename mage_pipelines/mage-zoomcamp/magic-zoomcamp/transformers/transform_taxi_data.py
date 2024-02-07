import re

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(data, *args, **kwargs):
    
    print(f"Rows without passengers: {data['passenger_count'].fillna(0).isin([0]).sum() }")
    print(f"Rows with trips with no distances : {data['trip_distance'].fillna(0).isin([0]).sum() }")

    data['lpep_pickup_date'] = data['lpep_pickup_datetime'].dt.date

    #Rename camel case to snake case
    data.columns = [re.sub('([a-z0-9])([A-Z])', r'\1_\2', col).lower() for col in data.columns]
    print(data.columns)

    return data[~((data['passenger_count'] == 0) | (data['trip_distance'] == 0))]


@test
def test_output(output, *args) -> None:

    # Assertions
    assert 'vendor_id' in output.columns, "Assertion Error: 'vendor_id' not found in columns"
    assert (output['passenger_count'] > 0).all(), "Assertion Error: passenger_count should be greater than 0"
    assert (output['trip_distance'] > 0).all(), "Assertion Error: trip_distance should be greater than 0"
