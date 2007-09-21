
from nose.tools import with_setup, eq_, raises
from fixture import DataSet
from fixture.dataset import DataRow, SuperSet, MergedSuperSet
from fixture.test import attr

class Books(DataSet):
    def data(self):
        return (
            ('lolita', dict(title='lolita')),
            ('pi', dict(title='life of pi')),
        )

class Movies(DataSet):
    def data(self):
        return (
            ('peewee', dict(director='Tim Burton')),
            ('aquatic', dict(director='cant remember his name')),
        )
        
        
class Authors(DataSet):
    class martel:
        name = 'Yann Martel'
    class nabokov:
        name = 'Vladimir Nabokov'
class BooksAndAuthors(DataSet):
    class lolita:
        title = 'lolita'
        author = Authors.nabokov.ref('name')
    class pi:
        title = 'life of pi'
        author = Authors.martel.ref('name')
        


class CategoryData(DataSet):
    class vehicles:
        id = 1
        name = 'cars'
    class free_stuff:
        id = 2
        name = 'get free stuff'
    class discounted:
        id = 3
        name = 'discounted stuff'
        
class ProductData(DataSet):
    class truck:
        id = 1
        name = 'truck'
        category_id = CategoryData.vehicles.ref('id')
    class spaceship:
        id = 2
        name = 'spaceship'
        category_id = CategoryData.vehicles.ref('id')

class OfferData(DataSet):
    class free_truck:
        id = 1
        name = "it's a free truck"
        product_id = ProductData.truck.ref('id')
        category_id = CategoryData.free_stuff.ref('id')
    class discounted_spaceship:
        id = 2
        name = "it's a spaceship 1/2 off"
        product_id = ProductData.spaceship.ref('id')
        category_id = CategoryData.discounted.ref('id')

class DataSetTest:
    """tests behavior of a DataSet object."""
    def assert_access(self, dataset):
        raise NotImplementedError
    def assert_row_dict_for_iter(self, items, count):
        raise NotImplementedError
    def assert_itered_n_times(count):
        raise NotImplementedError
    
    @attr(unit=1)
    def test_access(self):
        self.assert_access(self.dataset)

    @attr(unit=1)
    def test_iter_yields_keys_rows(self):
        count=0
        for k, row in self.dataset:
            count += 1
            items = dict([(k, getattr(row, k)) for k in row.columns()])
            self.assert_row_dict_for_iter(k, items, count)
        
        self.assert_itered_n_times(count)

class TestDataSet(DataSetTest):
    def setUp(self):
        self.dataset = Books()
    
    def assert_access(self, dataset):
        eq_(dataset.lolita.title, 'lolita')
        eq_(dataset.pi.title, 'life of pi')
        eq_(dataset['lolita'].title, 'lolita')
        eq_(dataset['pi'].title, 'life of pi')
    
    def assert_itered_n_times(self, count):
        eq_(count, 2)
    
    def assert_row_dict_for_iter(self, key, items, count):        
        if count == 1:
            eq_(items, {'title': 'lolita'})
        elif count == 2:
            eq_(items, {'title': 'life of pi'})
        else:
            raise ValueError("unexpected row %s, count %s" % (items, count))

@attr(unit=1)
def test_DataSet_explains_how_to_properly_access_id():
    class FooData(DataSet):
        class foo:
            name = 'foo'
    try:
        FooData.foo.id
    except AttributeError, e:
        eq_(str(e), (
            "FooData.foo.id is only available after the DataSet is loaded.  "
            "Then, you will need to access it from a loaded data object "
            "instead, like: data.FooData.foo.id, or the equivalent."))
    else:
        raise AssertionError("expected AttributeError to be raised")

class TestDataTypeDrivenDataSet(TestDataSet):
    def setUp(self):
        class Books(DataSet):
            class lolita:
                title = 'lolita'
            class pi:
                title = 'life of pi'
        self.dataset = Books()

class EventData(DataSet):
    class click:
        type = 'click'
        session = 'aaaaaaa'
        offer = 1
        time = 'now'
    
    class submit(click):
        type = 'submit'
    class order(click):
        type = 'order'
    class activation(click):
        type = 'activation'

class TestInheritedRows(DataSetTest):
    def setUp(self):
        self.dataset = EventData()
    
    def assert_access(self, dataset):
        def assert_all_attr(type):
            fxt = getattr(dataset, type)
            eq_(fxt.type, type)
            eq_(fxt.session, 'aaaaaaa')
            eq_(fxt.offer, 1)
            eq_(fxt.time, 'now')
        
        assert_all_attr('click')
        assert_all_attr('submit')
        assert_all_attr('order')
        assert_all_attr('activation')
    
    def assert_itered_n_times(self, count):
        eq_(count, 4)
    
    def assert_row_dict_for_iter(self, key, items, count):
        if count == 1:
            eq_(items, 
                {'offer': 1, 'time': 'now', 'session': 'aaaaaaa', 
                'type': 'activation'})
        elif count == 2:
            eq_(items, 
                {'offer': 1, 'time': 'now', 'session': 'aaaaaaa', 
                'type': 'click'})
        elif count == 3:
            eq_(items, 
                {'offer': 1, 'time': 'now', 'session': 'aaaaaaa', 
                'type': 'order'})
        elif count == 4:
            eq_(items, 
                {'offer': 1, 'time': 'now', 'session': 'aaaaaaa', 
                'type': 'submit'})
        else:
            raise ValueError("unexpected row %s at key %s, count %s" % (
                                                        items, key, count))

class TestDataSetCustomMeta(DataSetTest):
    def setUp(self):
        # a dataset with a config that doesn't inherit from
        # the default config.  should be ok
        class Chairs(DataSet):
            class Meta:
                storable_name = 'PretendStorage'
            def data(self):
                return (
                    ('recliner', dict(type='recliner')),
                    ('Lazy-boy', dict(type='Lazy-boy'))
                )
        self.dataset = Chairs()
    
    def assert_access(self, dataset):
        eq_(dataset.recliner.type, 'recliner')
        eq_(dataset['Lazy-boy'].type, 'Lazy-boy')
        
        # should also have the same defaults as DataSet :
        eq_(dataset.meta.storable_name, 'PretendStorage')
        eq_(dataset.meta.row, DataSet.Meta.row)
    
    def assert_itered_n_times(self, count):
        eq_(count, 2)
    
    def assert_row_dict_for_iter(self, key, items, count):        
        if count == 1:
            eq_(items, {'type': 'recliner'})
        elif count == 2:
            eq_(items, {'type': 'Lazy-boy'})
        else:
            raise ValueError("unexpected row %s, count %s" % (items, count))
    
class SuperSetTest:
    """tests common behavior of SuperSet like objects."""
    SuperSet = None
    
    def setUp(self):
        self.superset = self.SuperSet(Books(), Movies())
    
    @attr(unit=True)
    def test_access(self):
        raise NotImplementedError
    
    @attr(unit=True)
    def test_iter_yields_datasets(self):
        count=0
        for ds in self.superset:
            count += 1
            if count == 1:
                eq_(ds.lolita.title, 'lolita')
            elif count == 2:
                eq_(ds.peewee.director, 'Tim Burton')
            else:
                raise ValueError("unexpected row %s, count %s" % (ds, count))
        eq_(count, 2)

class TestSuperSet(SuperSetTest):
    SuperSet = SuperSet
    
    @attr(unit=True)
    def test_access(self):
        eq_(self.superset.Books.lolita.title, 'lolita')
        eq_(self.superset.Books.pi.title, 'life of pi')
        eq_(self.superset.Books['lolita'].title, 'lolita')
        eq_(self.superset.Books['pi'].title, 'life of pi')
        eq_(self.superset['Books']['lolita'].title, 'lolita')
        eq_(self.superset['Books']['pi'].title, 'life of pi')

class TestMergedSuperSet(SuperSetTest):
    SuperSet = MergedSuperSet
    
    @attr(unit=True)
    def test_access(self):
        eq_(self.superset.lolita.title, 'lolita')
        eq_(self.superset.pi.title, 'life of pi')
        eq_(self.superset['lolita'].title, 'lolita')
        eq_(self.superset['pi'].title, 'life of pi')
        eq_(self.superset.peewee.director, 'Tim Burton')
        eq_(self.superset.aquatic.director, 'cant remember his name')
        
class TestComplexRefs:
    def setUp(self):
        self.offer_data = OfferData()
        self.product_data = ProductData()
    
    @attr(unit=True)
    def test_construction(self):
        eq_(self.offer_data.meta.references, [CategoryData, ProductData])
        eq_(self.product_data.meta.references, [CategoryData])
        
        cat_data = self.product_data.meta.references[0]()
        eq_(cat_data.meta.references, [])
        
        eq_([c.__class__ for c in self.product_data.ref], [CategoryData])