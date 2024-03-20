if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(data, *args, **kwargs):
    """
    Template code for a transformer block.

    Add more parameters to this function if this block has multiple parent blocks.
    There should be one parameter for each output variable from each parent block.

    Args:
        data: The output from the upstream parent block
        args: The output from any additional upstream blocks (if applicable)

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    # Specify your transformation logic here
    
    # Convert column names to lowercase and replace spaces with underscores
    print('Renaming columns...')
    data.rename(columns=lambda x: x.lower().replace(' ', '_'), inplace=True)
    print('--------------------------------')
    print('Renamed columns:', data.columns)
    print('--------------------------------')

    # check for missing values
    print('Checking for missing values in data...')
    print(data.isnull().sum())
    print('--------------------------------')
    
    # Data cleaning
    print('Taking care of Null/Missing values...')
    data['supplier'] = data['supplier'].fillna("NO SUPPLIER")
    data['item_type'] = data['item_type'].fillna("NO ITEM TYPE")
    data['retail_sales'] = data['retail_sales'].fillna(-1)

    return data


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
    
    # Check for null values in the specified columns
    assert not output['supplier'].isnull().any(), 'Error: There must be NO Missing values in "supplier" column'
    assert not output['item_type'].isnull().any(), 'Error: There must be NO Missing values in "item_type" column'
    assert not output['retail_sales'].isnull().any(), 'Error: There must be NO Missing values in "retail_sales" column'