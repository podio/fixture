
"""sqlalchemy fixture components."""

from fixture.loadable import DBLoadableFixture
from fixture.exc import UninitializedError

def negotiated_medium(obj, dataset):
    if is_table(obj):
        return TableMedium(obj, dataset)
    elif is_assigned_mapper(obj):
        return MappedClassMedium(obj, dataset)
    elif is_mapped_class(obj):
        return MappedClassMedium(obj, dataset)
    else:
        raise NotImplementedError("object %s is not supported by %s" % (
                                                    obj, SQLAlchemyFixture))

class SQLAlchemyFixture(DBLoadableFixture):
    """A fixture that knows how to load DataSet objects into sqlalchemy objects.
    
    Keyword Arguments
    -----------------
    - style
    
      - A Style object to translate names with
    
    - session_context
    
      - An instance of sqlalchemy.ext.sessioncontext.SessionContext.  A session 
        will be created from session_context.current
    
    - session
      
      - A session from sqlalchemy.create_session().  This will override the 
        session_context.current approach.
    
    - connection
    
      - A specific connectable/engine object (must be connected).  This is only 
        necessary if you are working with an unbound session *and* you want to 
        use the TableMedium for data storage.
    
    - dataclass
    
      - SuperSet to represent loaded data with
    
    - env
    
      - A dict or module that contains either mapped classes or Table objects,
        or both.  This will be search when style translates DataSet names into
        storage media.
    
    - medium
    
      - A custom StorageMediumAdapter to instantiate when storing a DataSet.
        By default, a medium adapter will be negotiated based on the type of 
        sqlalchemy object so you should only set this if you know what you 
        doing.
    
    """
    Medium = staticmethod(negotiated_medium)
    
    def __init__(self,  session=None, session_context=None, 
                        connection=None, **kw):
        DBLoadableFixture.__init__(self, **kw)
        self.session = session
        self.session_context = session_context
        self.connection = connection
    
    def begin(self, unloading=False):
        if self.session is None and self.session_context is None:
            raise UninitializedError(
                "%s must be assigned either a session or session_context" % (
                    self.__class__.__name__))
        if self.session is None:
            self.session = self.session_context.current
        if not self.connection and self.session.bind_to is not None:
            self.connection = self.session.bind_to.connect()
        
        DBLoadableFixture.begin(self, unloading=unloading)
        
    def clear_object(self, obj):
        raise NotImplementedError
    
    def commit(self):
        self.session.flush()
        DBLoadableFixture.commit(self)
    
    def create_transaction(self):
        transaction = self.session.create_transaction()
        if self.connection:
            transaction.add(self.connection)
        return transaction
    
    def dispose(self):
        from fixture.dataset import dataset_registry
        dataset_registry.clear()
        if self.connection:
            self.connection.close()
        if self.session:
            if self.session.bind_to:
                self.session.bind_to.dispose()
            self.session.close()
        if self.transaction:
            self.transaction.close()
    
    def rollback(self):
        DBLoadableFixture.rollback(self)

class MappedClassMedium(DBLoadableFixture.StorageMediumAdapter):
    def __init__(self, *a,**kw):
        DBLoadableFixture.StorageMediumAdapter.__init__(self, *a,**kw)
        
    def clearall(self):
        from sqlalchemy.orm.mapper import object_mapper
        from sqlalchemy.orm import session
        from sqlalchemy.orm.unitofwork import UOWTransaction
        
        uowtrans = UOWTransaction(self.session.uow, self.session)
        
        # log.info("CLEARING stored objects for %s", self.dataset)
        tables_to_clear = [] # must be ordered
        for obj in self.dataset.meta._stored_objects:
            uowtrans.register_object(obj, isdelete=True)
            for c in [obj] + list(object_mapper(obj).cascade_iterator(
                                                        'delete', obj)):
                for mapper in \
                        object_mapper(c).base_mapper().polymorphic_iterator():
                    # task = uowtrans.get_task_by_mapper(mapper)
                    # for dep in task.polymorphic_dependencies:
                    #     print dep
                    # for t in task.polymorphic_todelete_elements:
                    #     print t.obj
                    for table in mapper.tables:
                        table_pair = (mapper, table)
                        if table_pair not in tables_to_clear:
                            tables_to_clear.append(table_pair)
                            
        # from UOWTransaction.execute()
        while True:
            ret = False
            for task in uowtrans.tasks.values():
                for up in list(task.dependencies):
                    if up.preexecute(uowtrans):
                        ret = True
            if not ret:
                break
        
        # head = uowtrans._sort_dependencies()
        # # print head
        # if head:
        #     print head.dump()
        #     # for elem in head.polymorphic_todelete_elements:
        #     #     print elem
        
        for mapper, table in tables_to_clear:
            self.session.execute(mapper, table.delete(), {})
    
    # def clear(self, obj):
    #     self.session.delete(obj)
    
    def visit_loader(self, loader):
        self.session = loader.session
        self.transaction = loader.transaction
        
    def save(self, row, column_vals):
        obj = self.medium()
        for c, val in column_vals:
            setattr(obj, c, val)
        self.session.save(obj)
        return obj
        
class TableMedium(DBLoadableFixture.StorageMediumAdapter):
    class LoadedTableRow(object):
        def __init__(self, table, inserted_key, connection):
            self.table = table
            self.inserted_key = [k for k in inserted_key]
            self.connection = connection
            self.row = None
        
        def __getattr__(self, col):
            if not self.row:
                if len(self.inserted_key) > 1:
                    raise NotImplementedError(
                        "%s does not support making a select statement with a "
                        "composite key, %s.  probably fixable" % (
                                            self.__class__.__name__, 
                                            self.table.primary_key))
                
                first_pk = [k for k in self.table.primary_key][0]
                id = getattr(self.table.c, first_pk.key)
                c = self.connection.execute(self.table.select(
                                                id==self.inserted_key[0]))
                self.row = c.fetchone()
            return getattr(self.row, col)
            
    def __init__(self, *a,**kw):
        DBLoadableFixture.StorageMediumAdapter.__init__(self, *a,**kw)
        
    def clear(self, obj):
        i=0
        for k in obj.table.primary_key:
            id = getattr(obj.table.c, k.key)
            stmt = obj.table.delete(id==obj.inserted_key[i])
            c = self.connection.execute(stmt)
            i+=1
    
    def visit_loader(self, loader):
        if loader.connection is None:
            raise UninitializedError(
                "The loader using %s() has a None type connection.  "
                "To fix this, either pass in the connection keyword or use "
                "a session bound to an engine" % (
                    self.__class__.__name__))
        self.connection = loader.connection
        
    def save(self, row, column_vals):
        from sqlalchemy.schema import Table
        if not isinstance(self.medium, Table):
            raise ValueError(
                "medium %s must be a Table instance" % self.medium)
                
        stmt = self.medium.insert()
        c = self.connection.execute(stmt, dict(list(column_vals)))
        primary_key = c.last_inserted_ids()
        if primary_key is None:
            raise NotImplementedError(
                    "what can we do with a None primary key?")
        table_keys = [k for k in self.medium.primary_key]
        inserted_keys = [k for k in primary_key]
        if len(inserted_keys) != len(table_keys):
            raise ValueError(
                "expected primary_key %s, got %s (using table %s)" % (
                                table_keys, inserted_keys, self.medium))
        
        return self.LoadedTableRow(self.medium, primary_key, self.connection)

def is_assigned_mapper(obj):
    from sqlalchemy.orm.mapper import Mapper
    return hasattr(obj, 'mapper') and isinstance(obj.mapper, Mapper)

def is_mapped_class(obj):
    from sqlalchemy import util
    return hasattr(obj, 'c') and isinstance(obj.c, util.OrderedProperties)

def is_table(obj):
    from sqlalchemy.schema import Table
    return isinstance(obj, Table)