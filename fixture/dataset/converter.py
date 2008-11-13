
"""Utilities for converting datasets."""

from fixture.dataset import DataRow

def _obj_items(obj):
    for name in dir(obj):
        if name.startswith('__'):
            continue
        yield name, getattr(obj, name)

def dataset_to_objects(dataset):
    """Converts a DataSet class or instance to an 
    object representation suitable for JSON.
    
    In other words, it converts a dataset to a python list of 
    python dictoraries.  You can then serialize that to JSON.
    
    Example::
    
        >>> from fixture import DataSet
        >>> from fixture.dataset.converter import dataset_to_objects
        >>> class ArtistData(DataSet):
        ...     class joan_jett:
        ...         name = "Joan Jett and the Black Hearts"
        ...     class ramones:
        ...         name = "The Ramones"
        ... 
        >>> dataset_to_objects(ArtistData)
        [{'name': 'Joan Jett and the Black Hearts'}, {'name': 'The Ramones'}]
    
    """
    if isinstance(dataset, type):
        # we got a class so make it an instance
        # so that rows are resolved
        dataset = dataset()
    objects = []
    for name, row in _obj_items(dataset):
        try:
            if not issubclass(row, DataRow):
                continue
        except TypeError:
            continue
        row_dict = {}
        for col, val in _obj_items(row):
            if col=='_reserved_attr' or callable(val):
                continue
            row_dict[col] = val
        objects.append(row_dict)
    return objects

if __name__ == '__main__':
    import doctest
    doctest.testmod()